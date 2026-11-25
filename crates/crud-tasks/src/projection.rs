use pioneer_crud_payloads::json_projection::{json_path_text, queryable_json};
use pioneer_entity::task;
use pioneer_protocol::TASK_REVISION_RETRY_PENDING;
use sea_orm::sea_query::{Expr, ExprTrait, SimpleExpr};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionWriteOutcome {
    Applied,
    NoopAlreadyStarted,
    NoopAlreadyTerminal,
    NoopDuplicateTerminal,
    InvariantViolation { reason: String },
}

pub fn finish_requested_expr() -> Expr {
    // Keep the SQL NULL and JSON null behavior of the Task metadata fence.
    Expr::cust_with_exprs(
        "($1 ->> 'finishRequest') IS NOT NULL",
        [Expr::col(pioneer_entity::task::Column::MetadataJson)],
    )
}

// The JSON error carries the effective terminality of a blocked revision.
// IS JSON keeps legacy malformed payloads on the ordinary repair/error path.
pub(crate) fn pending_revision_retry_predicate(
    status: SimpleExpr,
    error: SimpleExpr,
) -> SimpleExpr {
    let code = json_path_text(queryable_json(error.clone()), &["code"]);
    Expr::cust_with_exprs(
        "CASE WHEN $1 = 'blocked' AND $2 IS JSON OBJECT \
         THEN COALESCE($3 = $4, FALSE) ELSE FALSE END",
        [status, error, code, Expr::val(TASK_REVISION_RETRY_PENDING)],
    )
}

/// Translate only the blocked status for queries that filter public tasks.
pub(crate) fn effective_task_status_expr() -> Expr {
    let status = Expr::col((task::Entity, task::Column::Status));
    let pending = pending_revision_retry_predicate(
        status.clone(),
        Expr::col((task::Entity, task::Column::ErrorJson)),
    );
    Expr::case(pending.and(finish_requested_expr().not()), "waiting")
        .finally(status)
        .into()
}
