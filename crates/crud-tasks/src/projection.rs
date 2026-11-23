use sea_orm::sea_query::Expr;

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
