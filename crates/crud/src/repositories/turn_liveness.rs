use anyhow::{Context, Result};
use pioneer_entity::turn_liveness;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::Expr;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, ExprTrait, QueryFilter, Set};

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
    if let Some(existing) = turn_liveness::Entity::find_by_id(observation.turn_id.clone())
        .one(db)
        .await
        .context("failed to load turn_liveness row")?
    {
        if existing.last_activity_at > observation.observed_at
            || (existing.last_activity_at == observation.observed_at
                && existing.last_activity_sequence >= observation.activity_sequence)
        {
            return Ok(false);
        }

        let affected = turn_liveness::Entity::update_many()
            .col_expr(
                turn_liveness::Column::ThreadId,
                Expr::value(observation.thread_id),
            )
            .col_expr(
                turn_liveness::Column::LastActivitySequence,
                Expr::value(observation.activity_sequence),
            )
            .col_expr(
                turn_liveness::Column::LastActivityKind,
                Expr::value(observation.activity_kind),
            )
            .col_expr(
                turn_liveness::Column::LastActivityItemId,
                Expr::value(observation.item_id),
            )
            .col_expr(
                turn_liveness::Column::LastActivityItemType,
                Expr::value(observation.item_type),
            )
            .col_expr(
                turn_liveness::Column::LastActivityAt,
                Expr::value(observation.observed_at),
            )
            .col_expr(
                turn_liveness::Column::UpdatedAt,
                Expr::value(observation.observed_at),
            )
            .filter(turn_liveness::Column::TurnId.eq(observation.turn_id))
            .filter(
                turn_liveness::Column::LastActivityAt
                    .lt(observation.observed_at)
                    .or(turn_liveness::Column::LastActivityAt
                        .eq(observation.observed_at)
                        .and(
                            turn_liveness::Column::LastActivitySequence
                                .lt(observation.activity_sequence),
                        )),
            )
            .exec(db)
            .await
            .context("failed to update turn_liveness row")?
            .rows_affected
            > 0;

        return Ok(affected);
    }

    turn_liveness::Entity::insert(turn_liveness::ActiveModel {
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
    .exec(db)
    .await
    .context("failed to insert turn_liveness row")?;

    Ok(true)
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
