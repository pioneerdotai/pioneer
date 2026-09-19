//! SeaQuery builders for the bounded multi-table reads. Policy constants are
//! SQL literals so SQLite can prove partial-index predicates; IDs and time are
//! bound values. Query plans are checked against the real migration.
use super::{PAGE_ROWS, PAYLOAD_BUDGET};
use pioneer_entity::{
    cli_runtime_native_event as event, native_event_cleanup_job as job,
    native_event_cleanup_scheduler as scheduler, recovery_job as recovery, turn,
    turn_cli_runtime_attempt as attempt, turn_cli_runtime_binding as binding,
    turn_cli_runtime_execution_segment as segment, turn_event_projection_state as receipt,
    turn_event_projection_stream_state as projection,
};
use sea_orm::Value;
use sea_orm::sea_query::{
    CommonTableExpression, Expr, ExprTrait, Func, JoinType, Order, Query, SelectStatement,
    WithClause,
};

const METHODS: [&str; 7] = [
    "item/started",
    "item/completed",
    "item/agentMessage/delta",
    "item/commandExecution/outputDelta",
    "turn/diff/updated",
    "thread/tokenUsage/updated",
    "account/rateLimits/updated",
];
const TERMINAL: [&str; 3] = ["completed", "failed", "interrupted"];
fn literal(value: impl Into<Value>) -> Expr {
    Expr::Constant(value.into())
}
fn one() -> Expr {
    literal(1_i64)
}
fn in_literals<const N: usize>(column: Expr, values: [&str; N]) -> Expr {
    column.is_in(values.into_iter().map(literal))
}
fn cte(name: &'static str, query: SelectStatement) -> CommonTableExpression {
    CommonTableExpression::new()
        .table_name(name)
        .materialized(true)
        .query(query)
        .to_owned()
}
fn event_column(alias: Option<&'static str>, column: event::Column) -> Expr {
    match alias {
        Some(alias) => Expr::col((alias, column)),
        None => Expr::col(column),
    }
}
fn payload_bytes(alias: Option<&'static str>) -> Expr {
    Func::char_length(event_column(alias, event::Column::PayloadRedactedJson).cast_as("BLOB"))
        .into()
}
pub(super) fn candidate_predicate(alias: Option<&'static str>) -> Expr {
    event_column(alias, event::Column::TurnId)
        .is_not_null()
        .and(in_literals(
            event_column(alias, event::Column::NativeMethod),
            METHODS,
        ))
        .and(payload_bytes(alias).lte(literal(PAYLOAD_BUDGET)))
}
pub(super) fn discovery(now: i64) -> SelectStatement {
    let mut due = Query::select()
        .column(job::Column::TurnId)
        .from(job::Entity)
        .and_where(Expr::col(job::Column::State).eq(literal("queued")))
        .and_where(Expr::col(job::Column::AvailableAt).gt(literal(0_i64)))
        .and_where(Expr::col(job::Column::AvailableAt).lte(now))
        .order_by(job::Column::AvailableAt, Order::Asc)
        .order_by(job::Column::TurnId, Order::Asc)
        .limit(1)
        .to_owned();
    let new = Query::select()
        .column(job::Column::TurnId)
        .from(job::Entity)
        .and_where(Expr::col(job::Column::State).eq(literal("queued")))
        .and_where(Expr::col(job::Column::AvailableAt).eq(literal(0_i64)))
        .and_where(Expr::col(job::Column::LastServedAt).eq(literal(0_i64)))
        .order_by(job::Column::TurnId, Order::Asc)
        .limit(1)
        .to_owned();
    let served = Query::select()
        .column(job::Column::TurnId)
        .from(job::Entity)
        .and_where(Expr::col(job::Column::State).eq(literal("queued")))
        .and_where(Expr::col(job::Column::AvailableAt).eq(literal(0_i64)))
        .and_where(Expr::col(job::Column::LastServedAt).gt(literal(0_i64)))
        .order_by(job::Column::LastServedAt, Order::Asc)
        .order_by(job::Column::TurnId, Order::Asc)
        .limit(1)
        .to_owned();
    let due_id = Expr::col(("due", job::Column::TurnId));
    let new_id = Expr::col(("new_job", job::Column::TurnId));
    let served_id = Expr::col(("served_job", job::Column::TurnId));
    let choose_new =
        new_id
            .clone()
            .is_not_null()
            .and(served_id.clone().is_null().or(
                Expr::col(("scheduler", scheduler::Column::NewJobsSinceServed)).lt(literal(4_i64)),
            ));
    let with = WithClause::new()
        .cte(cte("due", due.take()))
        .cte(cte("new_job", new))
        .cte(cte("served_job", served))
        .to_owned();
    Query::select()
        .expr_as(
            Expr::case(due_id.clone().is_not_null(), due_id.clone())
                .case(choose_new.clone(), new_id.clone())
                .finally(served_id.clone()),
            job::Column::TurnId,
        )
        .expr_as(
            Expr::case(due_id.clone().is_not_null(), literal(None::<String>))
                .case(choose_new, literal("new"))
                .finally(literal("served")),
            "regular_lane",
        )
        .from_as(scheduler::Entity, "scheduler")
        .left_join("due", literal(true))
        .left_join("new_job", literal(true))
        .left_join("served_job", literal(true))
        .and_where(Expr::col(("scheduler", scheduler::Column::Singleton)).eq(one()))
        .and_where(
            due_id
                .is_not_null()
                .or(new_id.is_not_null())
                .or(served_id.is_not_null()),
        )
        .with_cte(with)
        .to_owned()
}
fn ready(turn_id: &str) -> SelectStatement {
    let turn_id_column = Expr::col(("t", turn::Column::Id));
    let attempt_blocker = Query::select()
        .expr(one())
        .from_as(attempt::Entity, "a")
        .and_where(Expr::col(("a", attempt::Column::TurnId)).eq(turn_id_column.clone()))
        .and_where(in_literals(
            Expr::col(("a", attempt::Column::Status)),
            ["starting", "running"],
        ))
        .to_owned();
    let segment_blocker = Query::select()
        .expr(one())
        .from_as(segment::Entity, "s")
        .and_where(Expr::col(("s", segment::Column::TurnId)).eq(turn_id_column.clone()))
        .and_where(Expr::col(("s", segment::Column::Status)).eq(literal("running")))
        .to_owned();
    let recovery_blocker = Query::select()
        .expr(one())
        .from_as(recovery::Entity, "r")
        .and_where(Expr::col(("r", recovery::Column::TurnId)).eq(turn_id_column.clone()))
        .and_where(
            in_literals(
                Expr::col(("r", recovery::Column::Status)),
                ["pending", "active"],
            )
            .or(Expr::col(("r", recovery::Column::ResolutionPending)).eq(one())),
        )
        .to_owned();
    let pending_receipt = Query::select()
        .expr(one())
        .from_as(receipt::Entity, "receipt")
        .and_where(
            Expr::col(("receipt", receipt::Column::TurnId))
                .eq(Expr::col(("p", projection::Column::TurnId))),
        )
        .and_where(
            Expr::col(("receipt", receipt::Column::Sequence)).gt(Expr::col((
                "p",
                projection::Column::ProjectedThroughSequence,
            ))),
        )
        .to_owned();
    let healthy_projection = Query::select()
        .expr(one())
        .from_as(projection::Entity, "p")
        .and_where(Expr::col(("p", projection::Column::TurnId)).eq(turn_id_column.clone()))
        .and_where(Expr::col(("p", projection::Column::Status)).eq(literal("healthy")))
        .and_where(
            Expr::col(("p", projection::Column::ProjectedThroughSequence)).gt(literal(0_i64)),
        )
        .and_where(Expr::exists(pending_receipt).not())
        .to_owned();
    Query::select()
        .column(("b", binding::Column::RuntimeId))
        .from_as(turn::Entity, "t")
        .join_as(
            JoinType::InnerJoin,
            binding::Entity,
            "b",
            Expr::col(("b", binding::Column::TurnId)).eq(turn_id_column.clone()),
        )
        .and_where(turn_id_column.eq(turn_id))
        .and_where(in_literals(
            Expr::col(("t", turn::Column::Status)),
            TERMINAL,
        ))
        .and_where(in_literals(
            Expr::col(("b", binding::Column::Status)),
            TERMINAL,
        ))
        .and_where(Expr::exists(attempt_blocker).not())
        .and_where(Expr::exists(segment_blocker).not())
        .and_where(Expr::exists(recovery_blocker).not())
        .and_where(Expr::exists(healthy_projection))
        .to_owned()
}
pub(super) fn candidates(turn_id: &str, selected: Option<&[String]>) -> SelectStatement {
    let source = if selected.is_some_and(|ids| ids.is_empty()) {
        Query::select()
            .expr_as(literal(None::<String>).cast_as("TEXT"), event::Column::Id)
            .expr_as(literal(None::<i64>).cast_as("INTEGER"), "payload_bytes")
            .and_where(literal(false))
            .to_owned()
    } else {
        let runtime = Query::select()
            .column(binding::Column::RuntimeId)
            .from("ready")
            .to_owned();
        let mut source = Query::select()
            .column(("event", event::Column::Id))
            .expr_as(payload_bytes(Some("event")), "payload_bytes")
            .from_as(event::Entity, "event")
            .and_where(Expr::col(("event", event::Column::TurnId)).eq(turn_id))
            .and_where(Expr::col(("event", event::Column::RuntimeId)).eq(Expr::from(runtime)))
            .and_where(candidate_predicate(Some("event")))
            .order_by(("event", event::Column::Id), Order::Asc)
            .limit(PAGE_ROWS as u64)
            .to_owned();
        if let Some(ids) = selected {
            source.and_where(Expr::col(("event", event::Column::Id)).is_in(ids.iter().cloned()));
        }
        source
    };
    Query::select()
        .columns([
            ("ready", "runtime_id"),
            ("candidates", "id"),
            ("candidates", "payload_bytes"),
        ])
        .from("ready")
        .left_join("candidates", literal(true))
        .order_by(("candidates", "id"), Order::Asc)
        .with_cte(
            WithClause::new()
                .cte(cte("ready", ready(turn_id)))
                .cte(cte("candidates", source))
                .to_owned(),
        )
        .to_owned()
}
pub(super) fn remaining(turn_id: &str, runtime_id: Option<&str>) -> SelectStatement {
    let any = Query::select()
        .expr(one())
        .from(event::Entity)
        .and_where(Expr::col(event::Column::TurnId).eq(turn_id))
        .and_where(candidate_predicate(None))
        .limit(1)
        .to_owned();
    let mut current = any.clone();
    current.and_where(Expr::col(event::Column::RuntimeId).eq(Expr::val(runtime_id)));
    Query::select()
        .expr_as(Expr::exists(current), "current_runtime_remaining")
        .expr_as(Expr::exists(any), "any_remaining")
        .to_owned()
}
