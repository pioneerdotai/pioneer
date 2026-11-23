//! Publish Task producer changes alongside a canonical Turn mutation.
use anyhow::Result;
use pioneer_protocol::TurnStatus;
use sea_orm::{ConnectionTrait, entity::prelude::DateTimeWithTimeZone};

pub async fn update_turn_status<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
    status: TurnStatus,
    error: Option<&str>,
    updated_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let Some(change) = pioneer_crud_conversations::turn_writes::update_turn_status(
        db, thread_id, turn_id, status, error, updated_at,
    )
    .await?
    else {
        return Ok(false);
    };
    publish_task_turn_change(db, &change).await?;
    Ok(true)
}

pub async fn publish_task_turn_change<C: ConnectionTrait>(
    db: &C,
    change: &pioneer_crud_conversations::turn_writes::TurnChange,
) -> Result<()> {
    let previous = change.previous.as_ref();
    let current = Some(&change.current);
    crate::task_child_producers::turn_change(db, previous, current).await?;
    crate::task_occurrence_producers::turn_change(db, previous, current).await
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
        !pioneer_crud_execution::native_cancellation_context::has_accepted(db, turn_id).await?,
        "blocked resume cannot clear an accepted cancellation fence"
    );
    let Some(change) =
        pioneer_crud_conversations::projection_stream::clear_confirmed_blocked_for_resume(
            db, thread_id, turn_id, now,
        )
        .await?
    else {
        return Ok(());
    };
    crate::task_child_producers::receipt_change(db, change.before.as_ref(), change.after.as_ref())
        .await?;
    Ok(())
}
