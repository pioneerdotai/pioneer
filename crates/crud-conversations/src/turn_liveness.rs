use anyhow::{Context, Result};
use pioneer_entity::turn_liveness;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, OnConflict};
use sea_orm::{ConnectionTrait, EntityTrait, ExprTrait, Set};

#[derive(Debug, Clone)]
pub struct TurnLivenessObservation {
    pub turn_id: String,
    pub thread_id: String,
    pub activity_sequence: i64,
    pub activity_kind: String,
    pub item_id: Option<String>,
    pub item_type: Option<String>,
    pub observed_at: DateTimeWithTimeZone,
}

pub async fn observe_activity<C: ConnectionTrait>(
    db: &C,
    observation: TurnLivenessObservation,
) -> Result<bool> {
    use turn_liveness::Column as C;
    let stored_time = Expr::col((turn_liveness::Entity, C::LastActivityAt));
    let stored_sequence = Expr::col((turn_liveness::Entity, C::LastActivitySequence));
    let incoming_time = Expr::col(("excluded", C::LastActivityAt));
    let incoming_sequence = Expr::col(("excluded", C::LastActivitySequence));

    let affected = turn_liveness::Entity::insert(turn_liveness::ActiveModel {
        turn_id: Set(observation.turn_id),
        thread_id: Set(observation.thread_id),
        last_activity_sequence: Set(observation.activity_sequence),
        last_activity_kind: Set(observation.activity_kind),
        last_activity_item_id: Set(observation.item_id),
        last_activity_item_type: Set(observation.item_type),
        last_activity_at: Set(observation.observed_at),
        created_at: Set(observation.observed_at),
        updated_at: Set(observation.observed_at),
    })
    .on_conflict(
        OnConflict::column(C::TurnId)
            .update_columns([
                C::ThreadId,
                C::LastActivitySequence,
                C::LastActivityKind,
                C::LastActivityItemId,
                C::LastActivityItemType,
                C::LastActivityAt,
                C::UpdatedAt,
            ])
            .action_and_where(
                stored_time.clone().lt(incoming_time.clone()).or(stored_time
                    .eq(incoming_time)
                    .and(stored_sequence.lt(incoming_sequence))),
            )
            .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .context("failed to observe turn activity")?;
    Ok(affected == 1)
}

pub async fn find_by_turn_id<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Option<turn_liveness::Model>> {
    turn_liveness::Entity::find_by_id(turn_id.to_owned())
        .one(db)
        .await
        .context("failed to query turn_liveness by turn id")
}
