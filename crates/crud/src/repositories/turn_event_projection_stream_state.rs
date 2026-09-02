use anyhow::{Context, Result, bail};
use pioneer_entity::turn_event_projection_stream_state;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, OnConflict};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, Set};

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
            projected_through_sequence: Set(0),
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

    find(db, turn_id).await?.with_context(|| {
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

pub async fn advance_projected_through<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    expected_current_sequence: i64,
    projected_through_sequence: i64,
    updated_at: DateTimeWithTimeZone,
) -> Result<bool> {
    if expected_current_sequence < 0 || projected_through_sequence <= expected_current_sequence {
        bail!(
            "projection watermark for Turn `{turn_id}` cannot advance from `{expected_current_sequence}` through `{projected_through_sequence}`"
        );
    }

    Ok(turn_event_projection_stream_state::Entity::update_many()
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
        .exec(db)
        .await
        .with_context(|| {
            format!(
                "failed to advance projection watermark for Turn `{turn_id}` from `{expected_current_sequence}` through `{projected_through_sequence}`"
            )
        })?
        .rows_affected
        > 0)
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
        return Ok(true);
    }

    let current = find(db, turn_id)
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
    Ok(turn_event_projection_stream_state::Entity::update_many()
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
        > 0)
}
