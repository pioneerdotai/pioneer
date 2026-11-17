//! The single Gateway SeaORM callback installation path. Kept separate from
//! bootstrap so private SDK tests can use real callbacks without pool observers.
use super::query_fingerprint::QueryFingerprintLimiter;
use sea_orm::DatabaseConnection;
use std::sync::Arc;

/// Installs the callback and returns the weak pool observer for the caller to
/// register. The callback alone owns the strong lifetime anchor, as before.
pub(super) fn install_query_observer(
    connection: &mut DatabaseConnection,
    role: pioneer_observability::DatabaseRole,
    query_fingerprints: Arc<QueryFingerprintLimiter>,
) -> impl Fn() -> pioneer_observability::DatabasePoolSnapshot + Send + Sync + 'static + use<> {
    // SeaORM's callback belongs to the connection wrapper, not the SQLx pool.
    // Keep this anchor there; the global metrics registry must not own a pool
    // (and hence its native writer ownership) after bootstrap is discarded.
    let pool_lifetime = Arc::new(connection.get_sqlite_connection_pool().clone());
    let observed_pool = Arc::downgrade(&pool_lifetime);
    connection.set_metric_callback(move |info| {
        let _pool_lifetime = &pool_lifetime;
        let (enabled, generation) = pioneer_observability::telemetry_consent_snapshot();
        if !enabled {
            return;
        }
        let operation = sqlite_operation(info.statement.sql.as_str());
        let workload = current_database_workload();
        let workload_name =
            workload.unwrap_or(pioneer_observability::DatabaseWorkload::Unclassified);
        let fingerprints = query_fingerprints.metric_fingerprint(
            info.statement.sql.as_str(),
            role,
            operation,
            workload_name,
            info.failed,
        );
        pioneer_observability::query_cpu::record_fingerprint(fingerprints.canonical);
        pioneer_observability::turn_startup::record_history_sql(
            fingerprints.canonical,
            info.elapsed,
        );
        let query_fingerprint = fingerprints.metric;
        record_database_query(
            query_fingerprint,
            database_query_kind(operation),
            info.elapsed,
        );
        fingerprints.series.record_for_consent(
            pioneer_observability::DatabaseOperationMetric {
                role,
                operation,
                workload: workload_name,
                query_fingerprint,
                elapsed: info.elapsed,
                failed: info.failed,
            },
            Some(generation),
        );
    });
    move || {
        let Some(pool) = observed_pool.upgrade() else {
            return pioneer_observability::DatabasePoolSnapshot { size: 0, idle: 0 };
        };
        pioneer_observability::DatabasePoolSnapshot {
            size: u64::from(pool.size()),
            idle: u64::try_from(pool.num_idle()).unwrap_or(u64::MAX),
        }
    }
}

fn database_query_kind(
    operation: pioneer_observability::DatabaseOperation,
) -> pioneer_observability::DatabaseQueryKind {
    use pioneer_observability::{DatabaseOperation, DatabaseQueryKind};

    match operation {
        DatabaseOperation::Select | DatabaseOperation::Pragma => DatabaseQueryKind::Read,
        DatabaseOperation::Insert
        | DatabaseOperation::Update
        | DatabaseOperation::Delete
        | DatabaseOperation::Replace
        | DatabaseOperation::Transaction
        | DatabaseOperation::Schema => DatabaseQueryKind::Write,
        DatabaseOperation::Other => DatabaseQueryKind::Other,
    }
}

pub(super) fn sqlite_operation(sql: &str) -> pioneer_observability::DatabaseOperation {
    use pioneer_observability::DatabaseOperation;

    let token = sql
        .trim_start()
        .split(|character: char| !character.is_ascii_alphabetic())
        .next()
        .unwrap_or_default();
    if token.eq_ignore_ascii_case("select") || token.eq_ignore_ascii_case("explain") {
        DatabaseOperation::Select
    } else if token.eq_ignore_ascii_case("insert") {
        DatabaseOperation::Insert
    } else if token.eq_ignore_ascii_case("update") {
        DatabaseOperation::Update
    } else if token.eq_ignore_ascii_case("delete") {
        DatabaseOperation::Delete
    } else if token.eq_ignore_ascii_case("replace") {
        DatabaseOperation::Replace
    } else if token.eq_ignore_ascii_case("begin")
        || token.eq_ignore_ascii_case("commit")
        || token.eq_ignore_ascii_case("rollback")
        || token.eq_ignore_ascii_case("savepoint")
        || token.eq_ignore_ascii_case("release")
    {
        DatabaseOperation::Transaction
    } else if token.eq_ignore_ascii_case("create")
        || token.eq_ignore_ascii_case("alter")
        || token.eq_ignore_ascii_case("drop")
        || token.eq_ignore_ascii_case("vacuum")
        || token.eq_ignore_ascii_case("reindex")
        || token.eq_ignore_ascii_case("analyze")
        || token.eq_ignore_ascii_case("attach")
        || token.eq_ignore_ascii_case("detach")
    {
        DatabaseOperation::Schema
    } else if token.eq_ignore_ascii_case("pragma") {
        DatabaseOperation::Pragma
    } else {
        DatabaseOperation::Other
    }
}
pub(crate) fn current_database_workload() -> Option<pioneer_observability::DatabaseWorkload> {
    pioneer_observability::work::current_database().map(|context| context.workload())
}

pub(crate) fn record_database_query(
    fingerprint: u64,
    kind: pioneer_observability::DatabaseQueryKind,
    elapsed: std::time::Duration,
) {
    if let Some(context) = pioneer_observability::work::current_database() {
        context.record_query(fingerprint, kind, elapsed);
        pioneer_observability::live::progress(pioneer_observability::live::Stage::Database);
    }
}
