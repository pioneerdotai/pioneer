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
