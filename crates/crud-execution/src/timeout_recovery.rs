use anyhow::{Context, Result};
use pioneer_entity::turn_item_attempt;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::Expr;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter};

pub async fn suppress_timeout_recovery<C: ConnectionTrait>(
    db: &C,
    attempt_id: &str,
    reason: &str,
    context: serde_json::Value,
    updated_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let affected = turn_item_attempt::Entity::update_many()
        .col_expr(
            turn_item_attempt::Column::RecoverySuppressedReason,
            Expr::value(Some(reason.to_owned())),
        )
        .col_expr(
            turn_item_attempt::Column::RecoverySuppressedAt,
            Expr::value(Some(updated_at)),
        )
        .col_expr(
            turn_item_attempt::Column::RecoverySuppressionContextJson,
            Expr::value(context),
        )
        .col_expr(
            turn_item_attempt::Column::UpdatedAt,
            Expr::value(updated_at),
        )
        .filter(turn_item_attempt::Column::Id.eq(attempt_id.to_owned()))
        .filter(turn_item_attempt::Column::Status.eq("timed_out"))
        .filter(turn_item_attempt::Column::RecoveryAction.is_null())
        .filter(turn_item_attempt::Column::RecoverySuppressedReason.is_null())
        .exec(db)
        .await
        .with_context(|| format!("failed to suppress timeout recovery for attempt `{attempt_id}`"))?
        .rows_affected
        > 0;

    Ok(affected)
}
