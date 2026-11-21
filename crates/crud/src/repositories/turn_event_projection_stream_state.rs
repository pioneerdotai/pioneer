use anyhow::{Context, Result, bail};
use pioneer_entity::turn_event_projection_stream_state;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, OnConflict};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QuerySelect, Set};

pub use pioneer_crud_conversations::projection_stream::{
    STREAM_STATUS_HEALTHY, STREAM_STATUS_QUARANTINED, ensure_healthy, find,
    find_detached_work_tombstone,
};

pub async fn advance_projected_through<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    expected_current_sequence: i64,
    projected_through_sequence: i64,
    updated_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let Some(new) = pioneer_crud_conversations::projection_stream::advance_projected_through(
        db,
        turn_id,
        expected_current_sequence,
        projected_through_sequence,
        updated_at,
    )
    .await?
    else {
        return Ok(false);
    };
    // The guarded UPDATE changes the watermark and preserves the other
    // facts used by Task-child receipt invalidation.
    let mut old = new.clone();
    old.projected_through_sequence = expected_current_sequence;
    super::task_child_producers::receipt_change(db, Some(&old), Some(&new)).await?;
    Ok(true)
}

/// Records the active causal blocker. Repeating the same transition is a
/// no-op; a different blocker on an already quarantined stream is an invariant
/// violation because successors never own stream quarantine.
pub async fn quarantine<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
    blocking_event_id: &str,
    last_error: String,
    quarantined_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let current = ensure_healthy(db, thread_id, turn_id, quarantined_at).await?;
    if current.thread_id != thread_id {
        bail!(
            "projection stream `{turn_id}` belongs to thread `{}`, not `{thread_id}`",
            current.thread_id
        );
    }
    if current.status == STREAM_STATUS_QUARANTINED {
        if current.blocking_event_id.as_deref() == Some(blocking_event_id) {
            return Ok(false);
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
        super::task_child_producers::receipt_change(db, Some(&current), Some(&new)).await?;
        return Ok(true);
    }

    let current = turn_event_projection_stream_state::Entity::find_by_id(turn_id)
        .lock_exclusive()
        .one(db)
        .await?
        .with_context(|| format!("projection stream `{turn_id}` disappeared during quarantine"))?;
    if current.status == STREAM_STATUS_QUARANTINED
        && current.blocking_event_id.as_deref() == Some(blocking_event_id)
    {
        return Ok(false);
    }
    bail!("projection stream `{turn_id}` changed concurrently during quarantine")
}

pub async fn restore<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    blocking_event_id: &str,
    restored_at: DateTimeWithTimeZone,
) -> Result<bool> {
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
        super::task_child_producers::receipt_change(db, old.as_ref(), new.as_ref()).await?;
    }
    Ok(changed)
}

pub use pioneer_crud_conversations::projection_receipts::has_accepted_terminal;

/// Called only for a newly inserted terminal event in its append transaction.
/// Existing canonical replay never writes this marker.
pub async fn accept_terminal<C: ConnectionTrait>(
    db: &C,
    event: &crate::AppendedTurnEvent,
) -> Result<()> {
    if !matches!(
        &event.payload,
        crate::CanonicalTurnEventPayload::TurnCompleted(_)
            | crate::CanonicalTurnEventPayload::TurnFailed(_)
            | crate::CanonicalTurnEventPayload::TurnBlocked(_)
    ) {
        return Ok(());
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
    super::task_child_producers::receipt_change(db, old.as_ref(), new.as_ref()).await?;
    Ok(())
}

/// Only the two atomic lawful Blocked resume operations call this helper.
/// Receipt/health restoration, owner change and watermark work cannot clear it.
pub async fn clear_confirmed_blocked_for_resume<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
    now: DateTimeWithTimeZone,
    blocked_turn: &pioneer_entity::turn::Model,
) -> Result<()> {
    // The caller already read/revalidated this row in the same writer transaction.
    anyhow::ensure!(
        blocked_turn.id == turn_id
            && blocked_turn.thread_id == thread_id
            && blocked_turn.status == "blocked",
        "terminal marker can only clear for durable Blocked resume"
    );
    anyhow::ensure!(
        !super::native_cancellation_context::has_accepted(db, turn_id).await?,
        "blocked resume cannot clear an accepted cancellation fence"
    );
    let Some(current) = turn_event_projection_stream_state::Entity::find_by_id(turn_id)
        .lock_exclusive()
        .one(db)
        .await?
    else {
        return Ok(());
    };
    anyhow::ensure!(
        current.thread_id == thread_id,
        "blocked resume crosses projection stream scope"
    );
    if current.accepted_terminal_event_id.is_none()
        && current.accepted_terminal_event_type.is_none()
        && current.accepted_terminal_sequence.is_none()
    {
        return Ok(());
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
    super::task_child_producers::receipt_change(db, Some(&current), new.as_ref()).await?;
    Ok(())
}
