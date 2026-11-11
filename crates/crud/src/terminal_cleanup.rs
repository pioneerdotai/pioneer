//! Cleanup wakes follow committed terminal state, including late event writes.
//! No process-local Turn status cache: another writer may finish/resume a Turn.
use pioneer_entity::turn;
use pioneer_sqlite::{SqliteDatabase, WorkChange};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect};

/// Exhaustive write-set classification: item/attempt/audit/window projection
/// changes no HTML lifetime dependency. Resource admission only adds holds;
/// resource release is published by its own lifecycle owner.
pub(crate) fn html_lifetime_turn(event: &crate::events::TurnEventPayload) -> Option<&str> {
    use crate::events::CanonicalTurnEventPayload::*;
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
pub(crate) async fn html_turn_lifetimes<C: ConnectionTrait>(
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

pub(crate) struct Change(Vec<WorkChange>);

impl Change {
    pub(crate) fn events(db: &SqliteDatabase) -> Self {
        Self(
            [
                "maintenance.native",
                "maintenance.receipts",
                "maintenance.zstd",
            ]
            .map(|domain| db.work_signal(domain).changing())
            .into(),
        )
    }

    pub(crate) fn native(db: &SqliteDatabase) -> Self {
        Self(vec![db.work_signal("maintenance.native").changing()])
    }

    pub(crate) fn payload(db: &SqliteDatabase) -> Self {
        Self(vec![db.work_signal("maintenance.zstd").changing()])
    }

    /// The caller's atomic write already proves a committed completed/failed/
    /// interrupted Turn. Publish directly, before any post-commit reads. A later
    /// resume is harmless: cleanup revalidates current eligibility itself.
    pub(crate) fn completed_terminal(self, changed: bool, turns: &[String]) {
        for change in self.0 {
            change.completed_keys(if changed { turns } else { &[] });
        }
    }

    pub(crate) async fn complete(self, db: &SqliteDatabase, changed: bool, turns: &[String]) {
        let relevant = if changed && !turns.is_empty() {
            // A bounded primary-key metadata probe after releasing the writer.
            // This also covers late projection/raw arrivals after terminalization.
            // Errors conservatively wake the owner; never turn a committed write
            // into a reported failure merely because this advisory read failed.
            turn::Entity::find()
                .select_only()
                .column(turn::Column::Id)
                .filter(turn::Column::Id.is_in(turns.iter().cloned()))
                .filter(turn::Column::Status.is_in(["completed", "failed", "interrupted"]))
                .into_tuple::<String>()
                .all(db)
                .await
                .ok()
        } else {
            Some(Vec::new())
        };
        // Cancellation/error before known completion drops WorkChange and keeps
        // uncertainty + writer settlement, including a late SQLite commit.
        for change in self.0 {
            match &relevant {
                Some(keys) => change.completed_keys(keys),
                None => change.completed(true),
            }
        }
    }
}
