use anyhow::{Context, Result};
use pioneer_entity::{agent_action as action, agent_action_outbox as outbox};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Alias, Expr, ExprTrait, Order, Query, SelectStatement, UnionType};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, FromQueryResult, QueryFilter, QueryOrder,
    QuerySelect, QueryTrait, Select,
};

const RANGE_LIMIT: u64 = 32;

// Keep these constants literal so PostgreSQL can prove implication of the
// partial-index predicates even for a generic prepared statement.
const IMMEDIATE: &str = "next_attempt_at IS NULL AND status IN ('pending','failed') AND attempts<8 AND (status='pending' OR last_error IS NULL OR last_error NOT IN ('outbox_start_integrity_rejected','outbox_start_authority_denied','outbox_start_runtime_unavailable','outbox_foreground_conflict','outbox_start_outcome_unknown')) AND (status!='failed' OR COALESCE(last_error,'')!='outbox_detached_binding_rejected')";
const TIMED: &str =
    "next_attempt_at IS NOT NULL AND (status='pending' OR (status='failed' AND attempts<8))";
const EXHAUSTED: &str = "status IN ('pending','failed') AND attempts=8 AND agent_action_outbox_start_candidate(payload_json)";
const EXHAUSTED_DUE: &str = "COALESCE(next_attempt_at, '-infinity'::timestamptz)";

fn exhausted() -> Select<outbox::Entity> {
    // The indexed predicate lexes the envelope, including strings containing
    // escaped NUL. The canonical action row authorizes reconciliation.
    let canonical = action::Entity::find()
        .select_only()
        .column(action::Column::ActionKind)
        .filter(
            Expr::col((action::Entity, action::Column::Id))
                .eq(Expr::col((outbox::Entity, outbox::Column::ActionId))),
        )
        .into_query();
    outbox::Entity::find()
        .filter(Expr::cust(EXHAUSTED))
        // The scalar lookup uses the action primary key for each candidate
        // reached by the partial outbox index. A semi-join can start by
        // scanning the entire action ledger before it applies the bound.
        .filter(Expr::from(canonical).eq("start_agent"))
        .filter(Expr::cust("(status!='failed' OR next_attempt_at IS NOT NULL OR COALESCE(last_error,'')!='outbox_detached_binding_rejected')"))
}

#[derive(FromQueryResult)]
struct Deadline {
    next_attempt_at: DateTimeWithTimeZone,
}

/// Return a future wakeup hint rounded up to a whole Unix second.
pub async fn next_deadline<C: ConnectionTrait>(
    db: &C,
    now: DateTimeWithTimeZone,
) -> Result<Option<i64>> {
    let ordinary = outbox::Entity::find()
        .select_only()
        .column(outbox::Column::NextAttemptAt)
        .filter(Expr::cust(TIMED))
        .filter(outbox::Column::NextAttemptAt.gt(now))
        .order_by_asc(outbox::Column::NextAttemptAt)
        .limit(1)
        .into_query();
    let reconciliation = exhausted()
        .select_only()
        .column(outbox::Column::NextAttemptAt)
        .filter(Expr::cust(EXHAUSTED_DUE).gt(now))
        .order_by_asc(Expr::cust(EXHAUSTED_DUE))
        .limit(1)
        .into_query();
    let heads = combine(ordinary, reconciliation, UnionType::All);
    let query = Query::select()
        .column(outbox::Column::NextAttemptAt)
        .from_subquery(heads, Alias::new("deadlines"))
        .order_by(outbox::Column::NextAttemptAt, Order::Asc)
        .limit(1)
        .to_owned();

    // SeaORM has no typed UNION of limited ORM selects. Build the set
    // operation with SeaQuery and decode it as a typed projection.
    Ok(
        Deadline::find_by_statement(db.get_database_backend().build(&query))
            .one(db)
            .await?
            .map(|row| {
                let at = row.next_attempt_at;
                at.timestamp()
                    .saturating_add(i64::from(at.timestamp_subsec_nanos() != 0))
            }),
    )
}

#[derive(FromQueryResult)]
struct Candidate {
    id: String,
}

/// Discover bounded immediate and timed work. The caller revalidates under the
/// outbox row lock before claiming each candidate.
pub async fn discover<C: ConnectionTrait>(
    db: &C,
    now: DateTimeWithTimeZone,
    limit: u64,
) -> Result<Vec<String>> {
    let immediate_limit = std::cmp::min(limit.div_ceil(2), RANGE_LIMIT);
    let timed_limit = std::cmp::min(limit / 2, RANGE_LIMIT);
    let mut ids = outbox::Entity::find()
        .select_only()
        .column(outbox::Column::Id)
        .filter(Expr::cust(IMMEDIATE))
        .order_by_asc(outbox::Column::CreatedAt)
        .order_by_asc(outbox::Column::Id)
        .limit(immediate_limit)
        .into_tuple::<String>()
        .all(db)
        .await
        .context("failed to list immediate agent action outbox IDs")?;

    let timed = candidate_range(
        outbox::Entity::find()
            .filter(Expr::cust(TIMED))
            .filter(outbox::Column::NextAttemptAt.lte(now)),
        Expr::col(outbox::Column::NextAttemptAt),
        timed_limit,
    );
    let reconciliation = candidate_range(
        exhausted().filter(Expr::cust(EXHAUSTED_DUE).lte(now)),
        Expr::cust(EXHAUSTED_DUE),
        timed_limit,
    );
    let candidates = combine(timed, reconciliation, UnionType::Distinct);
    let query = Query::select()
        .column(outbox::Column::Id)
        .from_subquery(candidates, Alias::new("candidates"))
        .order_by_expr(Expr::cust(EXHAUSTED_DUE), Order::Asc)
        .order_by(outbox::Column::CreatedAt, Order::Asc)
        .order_by(outbox::Column::Id, Order::Asc)
        .limit(timed_limit)
        .to_owned();
    ids.extend(
        Candidate::find_by_statement(db.get_database_backend().build(&query))
            .all(db)
            .await
            .context("failed to list timed agent start delivery/reconciliation IDs")?
            .into_iter()
            .map(|row| row.id),
    );
    // A concurrent update can move a row between the two independent reads.
    let mut seen = std::collections::HashSet::new();
    ids.retain(|id| seen.insert(id.clone()));
    Ok(ids)
}

fn candidate_range(query: Select<outbox::Entity>, due: Expr, limit: u64) -> SelectStatement {
    query
        .select_only()
        .columns([
            outbox::Column::Id,
            outbox::Column::NextAttemptAt,
            outbox::Column::CreatedAt,
        ])
        .order_by_asc(due)
        .order_by_asc(outbox::Column::CreatedAt)
        .order_by_asc(outbox::Column::Id)
        .limit(limit)
        .into_query()
}

fn combine(left: SelectStatement, right: SelectStatement, union: UnionType) -> SelectStatement {
    // Isolate each ORDER BY/LIMIT before combining the arms.
    Query::select()
        .expr(Expr::cust("*"))
        .from_subquery(left, Alias::new("ordinary"))
        .union(
            union,
            Query::select()
                .expr(Expr::cust("*"))
                .from_subquery(right, Alias::new("reconciliation"))
                .to_owned(),
        )
        .to_owned()
}
