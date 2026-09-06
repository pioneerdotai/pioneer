//! Restart-safe discovery of pre-existing canonical completions, not a second learner.
use anyhow::{Context, Result};
use pioneer_entity::{
    self_improvement_source_turn as source, self_improvement_workspace_state as state, thread,
    turn, turn_event,
};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, EntityTrait, JoinType, QueryFilter, QueryOrder,
    QuerySelect, Set,
};

use super::{
    canonical_turn_event, self_improvement_run, self_improvement_source_turn,
    self_improvement_workspace_state,
};
use crate::{CanonicalTurnEventPayload, CanonicalTurnEventRecord};

pub(crate) struct HistoryEvent {
    pub id: String,
    pub decoded: Option<CanonicalTurnEventRecord>,
}

pub(crate) async fn discover<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    after: Option<&str>,
    limit: u64,
) -> Result<Vec<HistoryEvent>> {
    let mut query = turn_event::Entity::find()
        .join(
            JoinType::InnerJoin,
            turn_event::Entity::belongs_to(thread::Entity)
                .from(turn_event::Column::ThreadId)
                .to(thread::Column::Id)
                .into(),
        )
        .filter(thread::Column::WorkspaceId.eq(workspace_id))
        .filter(thread::Column::OriginKind.is_in(self_improvement_source_turn::source_origins()))
        .filter(
            thread::Column::AccessClass
                .is_in(self_improvement_source_turn::source_access_classes()),
        )
        .filter(thread::Column::SidebarVisibility.eq("visible"))
        .filter(turn_event::Column::EventType.is_in(["turn/completed", "item/completed"]));
    if let Some(after) = after {
        query = query.filter(turn_event::Column::Id.gt(after));
    }
    // Event IDs need not be chronological: this is a one-time scan of pre-upgrade rows.
    // Concurrent new completions are already indexed by the live atomic projector.
    let rows = query
        .order_by_asc(turn_event::Column::Id)
        .limit(limit.min(32))
        .all(db)
        .await
        .context("failed to discover historical self-improvement events")?;
    // The bounded result is materialized and its reader permit released before JSON decoding.
    Ok(rows
        .into_iter()
        .map(|row| {
            let id = row.id.clone();
            let decoded = canonical_turn_event::decode_event(row, workspace_id).ok();
            if decoded.is_none() {
                tracing::warn!(
                    reason = "invalid_canonical_event",
                    "self-improvement history event skipped"
                );
            }
            HistoryEvent { id, decoded }
        })
        .collect())
}

/// Called inside a serialized maintenance write transaction. The canonical payload was decoded
/// outside the writer and is append-only. Revalidate existence and current workspace/turn/task
/// authority using the same bounded projection checks as live completion before inserting.
/// Insertion and scan checkpoint are atomic; cancellation/DB failure cannot consume a missing row.
pub(crate) async fn apply<C: ConnectionTrait>(
    db: &C,
    expected: &state::Model,
    event: Option<&HistoryEvent>,
) -> Result<bool> {
    let Some(current) = self_improvement_workspace_state::find(db, &expected.workspace_id).await?
    else {
        return Ok(false);
    };
    if current.effective_enabled_at.is_none()
        || current.activation_epoch != expected.activation_epoch
        || current.history_backfill_complete
        || current.history_backfill_after_event_id != expected.history_backfill_after_event_id
    {
        return Ok(false);
    }
    if let Some(event) = event {
        if let Some(decoded) = &event.decoded {
            let exists = turn_event::Entity::find_by_id(event.id.clone())
                .select_only()
                .column(turn_event::Column::Id)
                .into_tuple::<String>()
                .one(db)
                .await?
                .is_some();
            if exists {
                let prepared = match &decoded.payload {
                    CanonicalTurnEventPayload::TurnCompleted(notification) => {
                        let current = turn::Entity::find_by_id(notification.turn.id.clone()).one(db).await?;
                        if current.is_some_and(|turn| turn.thread_id == notification.thread_id
                            && turn.status == "completed" && turn.turn_kind == "conversation" && turn.origin == "user") {
                            self_improvement_source_turn::prepare_completed_source_turn(db, &event.id, decoded.created_at, notification).await
                        } else { Ok(None) }
                    }
                    CanonicalTurnEventPayload::ItemCompleted(notification) =>
                        self_improvement_source_turn::prepare_completed_collaborative_source_exchange(db, &event.id, decoded.created_at, notification).await,
                    _ => Ok(None),
                };
                match prepared {
                    Ok(Some(prepared)) => {
                        // A parent exchange is one source even when several children delivered.
                        let existing = source::Entity::find()
                            .filter(source::Column::TurnId.eq(&prepared.turn_id))
                            .one(db)
                            .await?;
                        if existing.is_none() {
                            self_improvement_source_turn::apply_prepared_source_turn(db, prepared)
                                .await?;
                        }
                    }
                    Ok(None) => {}
                    Err(error) if error.downcast_ref::<sea_orm::DbErr>().is_some() => {
                        return Err(error);
                    }
                    Err(_) => {
                        // Invalid legacy domain data is terminal for discovery, not an infinite
                        // retry barrier. Infrastructure errors above must roll back the checkpoint.
                        tracing::warn!(
                            reason = "invalid_source_authority",
                            "self-improvement history event skipped"
                        );
                    }
                }
            }
        }
    }
    let mut update: state::ActiveModel = current.into();
    if let Some(event) = event {
        update.history_backfill_after_event_id = Set(Some(event.id.clone()));
    } else {
        update.history_backfill_complete = Set(true);
    }
    update.update(db).await?;
    Ok(true)
}

/// Discovery can be stale, but rewinding farther is harmless. Revalidate cursor/epoch and the
/// absence of an unresolved frozen run so a concurrent finalization or retry is never reset.
pub(crate) async fn rewind_idle<C: ConnectionTrait>(
    db: &C,
    expected: &state::Model,
    earliest: i64,
) -> Result<()> {
    let Some(current) = self_improvement_workspace_state::find(db, &expected.workspace_id).await?
    else {
        return Ok(());
    };
    if current.effective_enabled_at.is_none()
        || current.activation_epoch != expected.activation_epoch
        || current.cursor_source_id != expected.cursor_source_id
        || earliest > current.cursor_source_id
        || self_improvement_run::find_oldest_unresolved(
            db,
            &current.workspace_id,
            current.activation_epoch,
        )
        .await?
        .is_some()
    {
        return Ok(());
    }
    let mut update: state::ActiveModel = current.into();
    update.cursor_source_id = Set(earliest - 1);
    update.update(db).await?;
    Ok(())
}
