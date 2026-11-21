use anyhow::Result;
use sea_orm::DatabaseTransaction;
use sea_orm::entity::prelude::DateTimeWithTimeZone;

#[cfg(test)]
pub use pioneer_crud_conversations::event_projection::claim_due;
pub use pioneer_crud_conversations::event_projection::{
    ClaimedTurnEventProjection, NewTurnEventProjectionState, PROJECTION_STATUS_EXHAUSTED,
    PROJECTION_STATUS_FAILED, PROJECTION_STATUS_PENDING, PROJECTION_STATUS_PROJECTED,
    PROJECTION_STATUS_PROJECTING, claim_candidates, discover, discover_for_turns, find_by_event_id,
    has_unprojected_event, insert_claimed, is_next_projection_sequence, is_projected,
    mark_exhausted_claimed, mark_failed_claimed, next_deadline, release_claim_as_pending,
    reset_exhausted_as_pending,
};

pub async fn mark_projected_claimed(
    db: &DatabaseTransaction,
    event_id: &str,
    turn_id: &str,
    sequence: i64,
    claim_token: &str,
    projected_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let affected = pioneer_crud_conversations::event_projection::mark_claim_projected(
        db,
        event_id,
        turn_id,
        sequence,
        claim_token,
        projected_at,
    )
    .await?;
    if affected
        && !super::turn_event_projection_stream_state::advance_projected_through(
            db,
            turn_id,
            sequence - 1,
            sequence,
            projected_at,
        )
        .await?
    {
        let stored_sequence = super::turn_event_projection_stream_state::find(db, turn_id)
            .await?
            .map(|stream| stream.projected_through_sequence);
        anyhow::bail!(
            "projection watermark for Turn `{turn_id}` did not advance atomically from `{}` through `{sequence}` while completing event `{event_id}`; stored watermark: {stored_sequence:?}",
            sequence - 1
        );
    }

    Ok(affected)
}
