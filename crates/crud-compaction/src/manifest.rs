use sea_orm::{ConnectionTrait, DbBackend, Statement};

// The installed SeaQuery CTE builder does not express a recursive PostgreSQL
// work-table reference followed by a lateral union of its successor edges.
const VALIDATE_SOURCES: &str = include_str!("manifest_sources.sql");

#[doc(hidden)]
pub fn source_validation_statement(operation: &str) -> Statement {
    Statement::from_sql_and_values(DbBackend::Postgres, VALIDATE_SOURCES, [operation.into()])
}

/// Raw inputs must match their current revisions. Historical coverage is
/// expanded only to establish that an inherited checkpoint is authorized by
/// delivery proofs or by the frozen basis. It never authorizes raw reads.
pub async fn sources_current<C: ConnectionTrait>(db: &C, operation: &str) -> anyhow::Result<bool> {
    Ok(db
        .query_one_raw(source_validation_statement(operation))
        .await?
        .is_none())
}
