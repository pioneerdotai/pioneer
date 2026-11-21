use anyhow::Result;
use pioneer_crud_conversations::projection_stream;
use pioneer_crud_tasks::task_child_producers;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::{ConnectionTrait, DatabaseTransaction};

pub async fn mark_projected_claimed(
    db: &DatabaseTransaction,
    event_id: &str,
    turn_id: &str,
    sequence: i64,
    claim_token: &str,
    projected_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let Some(change) = pioneer_crud_conversations::event_projection::mark_projected_claimed(
        db,
        event_id,
        turn_id,
        sequence,
        claim_token,
        projected_at,
    )
    .await?
    else {
        return Ok(false);
    };
    task_child_producers::receipt_change(db, change.before.as_ref(), change.after.as_ref()).await?;
    Ok(true)
}

pub async fn advance_projected_through<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    expected_current_sequence: i64,
    projected_through_sequence: i64,
    updated_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let Some(new) = projection_stream::advance_projected_through(
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
    task_child_producers::receipt_change(db, Some(&old), Some(&new)).await?;
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
    let Some(change) = projection_stream::quarantine(
        db,
        thread_id,
        turn_id,
        blocking_event_id,
        last_error,
        quarantined_at,
    )
    .await?
    else {
        return Ok(false);
    };
    task_child_producers::receipt_change(db, change.before.as_ref(), change.after.as_ref()).await?;
    Ok(true)
}

pub async fn restore<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    blocking_event_id: &str,
    restored_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let Some(change) =
        projection_stream::restore(db, turn_id, blocking_event_id, restored_at).await?
    else {
        return Ok(false);
    };
    task_child_producers::receipt_change(db, change.before.as_ref(), change.after.as_ref()).await?;
    Ok(true)
}

/// Called only for a newly inserted terminal event in its append transaction.
/// Existing canonical replay never writes this marker.
pub async fn accept_terminal<C: ConnectionTrait>(
    db: &C,
    event: &crate::AppendedTurnEvent,
) -> Result<()> {
    let Some(change) = projection_stream::accept_terminal(db, event).await? else {
        return Ok(());
    };
    task_child_producers::receipt_change(db, change.before.as_ref(), change.after.as_ref()).await?;
    Ok(())
}
