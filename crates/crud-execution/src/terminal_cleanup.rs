//! Cleanup wakes follow committed terminal state, including late event writes.
//! No process-local Turn status cache: another writer may finish/resume a Turn.
pub use pioneer_crud_signals::terminal_cleanup::Change;
use pioneer_entity::turn;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect};

/// Exhaustive write-set classification: item/attempt/audit/window projection
/// changes no HTML lifetime dependency. Resource admission only adds holds;
/// resource release is published by its own lifecycle owner.
pub fn html_lifetime_turn(
    event: &pioneer_crud_conversations::events::TurnEventPayload,
) -> Option<&str> {
    use pioneer_crud_conversations::events::CanonicalTurnEventPayload::*;
    match event {
        TurnStarted(_)
        | TurnCompleted(_)
        | TurnFailed(_)
        | TurnBlocked(_)
        | TurnMessageEdited(_)
        | TurnMessageDeleted(_) => Some(event.turn_id()),
        ItemStarted(_)
        | ItemCompleted(_)
        | ItemUpdated(_)
        | ItemTimeoutDetected(_)
        | ItemRecoveryOpened(_)
        | ItemRecoveryAttached(_)
        | ItemRetryScheduled(_)
        | ItemRetryAttemptStarted(_)
        | ItemRecoverySucceeded(_)
        | ItemRecoveryExhausted(_)
        | ItemToolRetryScheduled(_)
        | ItemToolRetryResolved(_)
        | ItemToolRetryExhausted(_)
        | TurnToolLoopBudgetExceeded(_)
        | TurnExecutionWindowStarted(_)
        | TurnExecutionWindowExhausted(_)
        | TurnExecutionWindowCheckpointed(_)
        | TurnExecutionWindowContinued(_)
        | TurnExecutionWindowBlocked(_)
        | TurnPermissionAudit(_) => None,
    }
}

/// Called before and after the relevant writes in the SAME transaction. Never
/// infer a dependency transition from the event DTO or a pre-transaction read.
pub async fn html_turn_lifetimes<C: ConnectionTrait>(
    db: &C,
    ids: &[String],
) -> Result<(Vec<(String, String, String)>, Vec<(String, String)>), sea_orm::DbErr> {
    if ids.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }
    let turns = turn::Entity::find()
        .select_only()
        .columns([
            turn::Column::Id,
            turn::Column::ThreadId,
            turn::Column::Status,
        ])
        .filter(turn::Column::Id.is_in(ids.iter().cloned()))
        .order_by_asc(turn::Column::Id)
        .into_tuple::<(String, String, String)>()
        .all(db)
        .await?;
    // Failed/blocked projection can remove a prepared finalization even if a
    // replay leaves the Turn status unchanged. That independently releases HTML.
    use pioneer_entity::turn_finalization as finalization;
    let finalizations = finalization::Entity::find()
        .select_only()
        .columns([finalization::Column::TurnId, finalization::Column::Status])
        .filter(finalization::Column::TurnId.is_in(ids.iter().cloned()))
        .order_by_asc(finalization::Column::TurnId)
        .into_tuple::<(String, String)>()
        .all(db)
        .await?;
    Ok((turns, finalizations))
}
