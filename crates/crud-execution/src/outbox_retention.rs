use anyhow::Result;
use chrono::Duration;
use pioneer_entity::{agent_action, agent_action_outbox};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::extension::postgres::PgExpr;
use sea_orm::sea_query::{Expr, ExprTrait, Query};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect,
};

use crate::outbox_claim::{AGENT_ACTION_OUTBOX_MAX_ATTEMPTS, AGENT_START_TERMINAL_FAILURE_CLASSES};

pub const AGENT_ACTION_LEDGER_PAYLOAD_RETENTION_DAYS: i64 = 30;

pub fn payload_not_compacted() -> Expr {
    // The existing marker predicate treats underscores as wildcards and
    // matches regardless of ASCII case.
    Expr::col(agent_action_outbox::Column::PayloadJson).not_ilike("%\"_pioneer_compacted\"%")
}

/// Compact bulky response/dispatch material only after the explicit replay
/// window and only for a terminal, complete action tuple. Immutable action,
/// receipt and outbox ownership facts remain queryable; exact payload hashes
/// still fence a late direct replay without retaining provider-sized blobs.
pub fn terminal_ledger_condition() -> Condition {
    Condition::any()
        .add(
            Condition::all()
                .add(agent_action_outbox::Column::Status.eq("delivered"))
                .add(agent_action_outbox::Column::DeliveredAt.is_not_null()),
        )
        .add(
            Condition::all()
                .add(agent_action_outbox::Column::Status.eq("failed"))
                .add(
                    Condition::any()
                        .add(
                            Condition::all()
                                .add(
                                    agent_action_outbox::Column::Attempts
                                        .gte(AGENT_ACTION_OUTBOX_MAX_ATTEMPTS),
                                )
                                // Exclude unresolved starts before LIMIT using
                                // a point lookup of the canonical action.
                                .add(sea_orm::sea_query::Expr::exists(
                                    Query::select()
                                        .column(agent_action::Column::Id)
                                        .from(agent_action::Entity)
                                        .and_where(
                                            sea_orm::sea_query::Expr::col((
                                                agent_action::Entity,
                                                agent_action::Column::Id,
                                            ))
                                            .equals((
                                                agent_action_outbox::Entity,
                                                agent_action_outbox::Column::ActionId,
                                            )),
                                        )
                                        .and_where(
                                            agent_action::Column::ActionKind.ne("start_agent"),
                                        )
                                        .to_owned(),
                                )),
                        )
                        .add(
                            agent_action_outbox::Column::LastError
                                .is_in(AGENT_START_TERMINAL_FAILURE_CLASSES),
                        ),
                )
                .add(agent_action_outbox::Column::NextAttemptAt.is_null()),
        )
}

/// Only the future retention head. Already-due rows belong to the bounded
/// cleanup pass, which is woken independently of this deadline.
pub async fn next_agent_ledger_retention_at<C: ConnectionTrait>(
    db: &C,
    now: DateTimeWithTimeZone,
) -> Result<Option<i64>> {
    let retention = Duration::days(AGENT_ACTION_LEDGER_PAYLOAD_RETENTION_DAYS);
    let cutoff = now - retention;
    let mut head = None::<DateTimeWithTimeZone>;
    // Equality on status gives each lane its own ordered created_at range.
    for status in ["delivered", "failed"] {
        let candidate = agent_action_outbox::Entity::find()
            .select_only()
            .column(agent_action_outbox::Column::CreatedAt)
            .filter(agent_action_outbox::Column::Status.eq(status))
            .filter(terminal_ledger_condition())
            .filter(payload_not_compacted())
            .filter(agent_action_outbox::Column::CreatedAt.gt(cutoff))
            .order_by_asc(agent_action_outbox::Column::CreatedAt)
            .order_by_asc(agent_action_outbox::Column::Id)
            .limit(1)
            .into_tuple::<DateTimeWithTimeZone>()
            .one(db)
            .await?;
        if let Some(candidate) = candidate
            && head.as_ref().is_none_or(|previous| candidate < *previous)
        {
            head = Some(candidate);
        }
    }
    Ok(head.map(|created| {
        let at = created + retention;
        at.timestamp()
            .saturating_add(i64::from(at.timestamp_subsec_nanos() != 0))
    }))
}
