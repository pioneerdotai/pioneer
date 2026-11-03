use anyhow::{Context, Result, bail};
use migration::Migrator;
use pioneer_config::AppConfig;
use pioneer_sqlite::{
    DEFAULT_LOCK_RETRY_ATTEMPTS, DEFAULT_LOCK_RETRY_BASE_DELAY_MS, DEFAULT_SQLITE_BUSY_TIMEOUT_MS,
    SqliteDatabase, SqliteWriteClass, SqliteWriteExecutor, is_anyhow_sqlite_lock,
    normalize_relative_database_file_name, retry_with_backoff, sqlite_connection_url,
    sqlite_read_only_connection_url,
};
use sea_orm::{ConnectOptions, Database, DatabaseConnection};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

#[derive(Debug, Clone)]
struct GatewayDatabaseRuntimeConfig {
    file_name: String,
    max_connections: u32,
    connect_timeout: Duration,
    acquire_timeout: Duration,
    idle_timeout: Duration,
    sqlx_logging: bool,
}

const MIN_READER_CONNECTIONS: u32 = 4;

impl GatewayDatabaseRuntimeConfig {
    fn from_app_config(config: &AppConfig) -> Result<Self> {
        let database = &config.gateway.database;

        let file_name = normalize_relative_database_file_name(
            database.file_name.as_str(),
            "gateway.database.file_name",
        )?;

        if database.max_connections == 0 {
            bail!("gateway.database.max_connections must be greater than 0");
        }
        if database.connect_timeout_ms == 0 {
            bail!("gateway.database.connect_timeout_ms must be greater than 0");
        }
        if database.acquire_timeout_ms == 0 {
            bail!("gateway.database.acquire_timeout_ms must be greater than 0");
        }
        if database.idle_timeout_ms == 0 {
            bail!("gateway.database.idle_timeout_ms must be greater than 0");
        }

        Ok(Self {
            file_name,
            max_connections: database.max_connections.max(MIN_READER_CONNECTIONS),
            connect_timeout: Duration::from_millis(database.connect_timeout_ms),
            acquire_timeout: Duration::from_millis(database.acquire_timeout_ms),
            idle_timeout: Duration::from_millis(database.idle_timeout_ms),
            sqlx_logging: database.sqlx_logging,
        })
    }
}

const WRITER_MAX_CONNECTIONS: u32 = 1;
#[path = "query_fingerprint.rs"]
mod query_fingerprint;
use query_fingerprint::QueryFingerprintLimiter;
#[cfg(test)]
use query_fingerprint::QueryFingerprintState;

/// Existing DB owner boundary for installer preflight. No writable connection,
/// migration execution, extension registration, startup repair or journal changes.
pub(crate) async fn inspect_applied_migration_names(
    runtime_home: &Path,
    app_config: &AppConfig,
) -> Result<Option<Vec<String>>> {
    let config = GatewayDatabaseRuntimeConfig::from_app_config(app_config)?;
    let path = gateway_database_path(runtime_home, app_config)?;
    // Only a positively absent database is a new installation. Metadata errors
    // (including denied access) must not become an empty applied set.
    match std::fs::symlink_metadata(&path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).context("migration inspection metadata unavailable"),
        Ok(metadata) => anyhow::ensure!(metadata.is_file(), "database is not a regular file"),
    }
    let mut options = connect_options(
        sqlite_read_only_connection_url(&path),
        1,
        &config,
        "pioneer-sqlite-reader-",
    );
    options.sqlx_logging(false);
    options.map_sqlx_sqlite_opts(|options| {
        options
            .read_only(true)
            .create_if_missing(false)
            .pragma("query_only", "ON")
            .thread_name(|id| format!("pioneer-sqlite-reader-{id}"))
    });
    let reader = Database::connect(options)
        .await
        .context("migration inspection unavailable")?;
    // Both handles are physically read-only; even an accidentally routed write
    // is rejected by SQLite. There is no additional writable executor/pool.
    let database = SqliteDatabase::new(reader.clone(), reader).maintenance();
    let result = async {
        database.validate_reader().await?;
        pioneer_crud::CrudStore::new(database.clone())
            .with_maintenance_access()
            .inspect_applied_migration_names()
            .await
    }
    .await;
    let closed = database.close().await;
    let names = result?;
    closed.context("failed to close migration inspection")?;
    Ok(Some(names))
}

pub async fn initialize(runtime_home: &Path, app_config: &AppConfig) -> Result<SqliteDatabase> {
    initialize_inner(runtime_home, app_config, None).await
}

pub async fn initialize_with_startup(
    runtime_home: &Path,
    app_config: &AppConfig,
    startup: &pioneer_observability::GatewayStartupTrace,
) -> Result<SqliteDatabase> {
    initialize_inner(runtime_home, app_config, Some(startup)).await
}

async fn initialize_inner(
    runtime_home: &Path,
    app_config: &AppConfig,
    startup: Option<&pioneer_observability::GatewayStartupTrace>,
) -> Result<SqliteDatabase> {
    let database_open_stage =
        startup.map(|trace| trace.stage(pioneer_observability::GatewayStartupStage::DatabaseOpen));
    let config = GatewayDatabaseRuntimeConfig::from_app_config(app_config)?;
    pioneer_sqlite::zstd::register_auto_extension_once()
        .context("failed to register sqlite-zstd gateway database extension")?;

    let database_path = runtime_home.join(config.file_name.as_str());
    let ownership = pioneer_sqlite::ownership::Opening::acquire(&database_path)?;
    let writer_url = sqlite_connection_url(ownership.path());
    let query_fingerprints = Arc::new(QueryFingerprintLimiter::default());

    let mut writer_options = connect_options(
        writer_url.clone(),
        WRITER_MAX_CONNECTIONS,
        &config,
        "pioneer-sqlite-writer-",
    );
    ownership.configure_with(&mut writer_options, |options| {
        options
            .busy_timeout(Duration::from_millis(DEFAULT_SQLITE_BUSY_TIMEOUT_MS))
            .thread_name(|id| format!("pioneer-sqlite-writer-{id}"))
    });
    let mut writer = Database::connect(writer_options)
        .await
        .with_context(|| format!("failed to connect to gateway writer `{writer_url}`"))?;
    let ownership = ownership.connected();
    if let Some(stage) = database_open_stage {
        stage.succeed();
    }

    let database_configure_stage = startup
        .map(|trace| trace.stage(pioneer_observability::GatewayStartupStage::DatabaseConfigure));
    configure_observability(
        &mut writer,
        pioneer_observability::DatabaseRole::Writer,
        WRITER_MAX_CONNECTIONS,
        query_fingerprints.clone(),
    );

    let writer = SqliteWriteExecutor::with_observer(writer, super::write_observer())
        .with_ownership(ownership);
    writer
        .ping(SqliteWriteClass::Maintenance)
        .await
        .context("failed to ping gateway writer")?;
    retry_with_backoff(
        || writer.apply_pragmas(SqliteWriteClass::Maintenance),
        is_anyhow_sqlite_lock,
        DEFAULT_LOCK_RETRY_ATTEMPTS,
        Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
    )
    .await?;
    if let Some(stage) = database_configure_stage {
        stage.succeed();
    }

    let database_migrate_stage = startup
        .map(|trace| trace.stage(pioneer_observability::GatewayStartupStage::DatabaseMigrate));
    retry_with_backoff(
        || async {
            writer
                .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, None)
                .await
                .context("failed to apply gateway database migrations")
        },
        is_anyhow_sqlite_lock,
        DEFAULT_LOCK_RETRY_ATTEMPTS,
        Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
    )
    .await?;
    if let Some(stage) = database_migrate_stage {
        stage.succeed();
    }

    let reader_url = sqlite_read_only_connection_url(database_path.as_path());
    let mut reader_options = connect_options(
        reader_url.clone(),
        config.max_connections,
        &config,
        "pioneer-sqlite-reader-",
    );
    reader_options.map_sqlx_sqlite_opts(|options| {
        options
            .read_only(true)
            .create_if_missing(false)
            .pragma("query_only", "ON")
            .thread_name(|id| format!("pioneer-sqlite-reader-{id}"))
    });
    let reader_open_stage = startup
        .map(|trace| trace.stage(pioneer_observability::GatewayStartupStage::DatabaseReaderOpen));
    let mut reader = Database::connect(reader_options)
        .await
        .with_context(|| format!("failed to connect to gateway read pool `{reader_url}`"))?;
    if let Some(stage) = reader_open_stage {
        stage.succeed();
    }
    let reader_configure_stage = startup.map(|trace| {
        trace.stage(pioneer_observability::GatewayStartupStage::DatabaseReaderConfigure)
    });
    configure_observability(
        &mut reader,
        pioneer_observability::DatabaseRole::Reader,
        config.max_connections,
        query_fingerprints,
    );
    if let Some(stage) = reader_configure_stage {
        stage.succeed();
    }

    let database =
        SqliteDatabase::from_executor_with_read_observer(reader, writer, super::read_observer());
    let reader_validate_stage = startup.map(|trace| {
        trace.stage(pioneer_observability::GatewayStartupStage::DatabaseReaderValidate)
    });
    database
        .maintenance()
        .validate_reader()
        .await
        .context("failed to validate gateway read pool")?;
    if let Ok(metadata) = std::fs::metadata(database_path.as_path()) {
        pioneer_observability::record_gateway_database_size_bytes(metadata.len());
    }
    if let Some(stage) = reader_validate_stage {
        stage.succeed();
    }

    info!(
        database_path = %database_path.display(),
        reader_connections = config.max_connections,
        writer_connections = WRITER_MAX_CONNECTIONS,
        "gateway database is ready"
    );

    Ok(database)
}

fn connect_options(
    database_url: String,
    max_connections: u32,
    config: &GatewayDatabaseRuntimeConfig,
    worker_prefix: &'static str,
) -> ConnectOptions {
    let mut options = ConnectOptions::new(database_url);
    options.max_connections(max_connections);
    options.connect_timeout(config.connect_timeout);
    options.acquire_timeout(config.acquire_timeout);
    options.idle_timeout(config.idle_timeout);
    options.sqlx_logging(config.sqlx_logging);
    // SQLx measures the complete wait for a pool permit/connection. A TRACE
    // event keeps normal logs quiet and is consumed by the observability layer.
    options.map_sqlx_sqlite_pool_opts(|pool_options| {
        pool_options.acquire_time_level(log::LevelFilter::Trace)
    });
    options.map_sqlx_sqlite_opts(move |sqlite_options| {
        sqlite_options
            .busy_timeout(Duration::from_millis(DEFAULT_SQLITE_BUSY_TIMEOUT_MS))
            .thread_name(move |id| format!("{worker_prefix}{id}"))
    });
    options
}

fn configure_observability(
    connection: &mut DatabaseConnection,
    role: pioneer_observability::DatabaseRole,
    max_connections: u32,
    query_fingerprints: Arc<QueryFingerprintLimiter>,
) {
    connection.set_metric_callback(move |info| {
        let (enabled, generation) = pioneer_observability::telemetry_consent_snapshot();
        if !enabled {
            return;
        }
        let operation = sqlite_operation(info.statement.sql.as_str());
        let workload = super::attribution::current_database_workload();
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
        let query_fingerprint = fingerprints.metric;
        super::attribution::record_database_query(
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
    let pool = connection.get_sqlite_connection_pool().clone();
    pioneer_observability::register_database_pool_observer(
        role,
        u64::from(max_connections),
        move || pioneer_observability::DatabasePoolSnapshot {
            size: u64::from(pool.size()),
            idle: u64::try_from(pool.num_idle()).unwrap_or(u64::MAX),
        },
    );
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

fn sqlite_operation(sql: &str) -> pioneer_observability::DatabaseOperation {
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

pub(crate) fn gateway_database_path(
    runtime_home: &Path,
    app_config: &AppConfig,
) -> Result<PathBuf> {
    let config = GatewayDatabaseRuntimeConfig::from_app_config(app_config)?;
    Ok(runtime_home.join(config.file_name.as_str()))
}

/// Real inspections never acquire writable ownership, run migrations or repair.
/// Both contours are physically read-only; accidental mutation fails closed.
pub(crate) async fn initialize_existing_for_inspection(
    runtime_home: &Path,
    app_config: &AppConfig,
) -> Result<Option<SqliteDatabase>> {
    let path = gateway_database_path(runtime_home, app_config)?;
    match std::fs::metadata(&path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).context("cannot inspect database path"),
        Ok(metadata) => anyhow::ensure!(metadata.is_file(), "database must be a regular file"),
    }
    pioneer_sqlite::zstd::register_auto_extension_once()?;
    let config = GatewayDatabaseRuntimeConfig::from_app_config(app_config)?;
    let mut options = connect_options(
        sqlite_read_only_connection_url(&path),
        1,
        &config,
        "pioneer-sqlite-inspection-",
    );
    options.map_sqlx_sqlite_opts(|options| {
        options
            .read_only(true)
            .create_if_missing(false)
            .pragma("query_only", "ON")
    });
    let reader = Database::connect(options)
        .await
        .context("cannot open read-only inspection")?;
    let database = SqliteDatabase::from_single_connection(reader).maintenance();
    database.validate_reader().await?;
    Ok(Some(database))
}

pub(crate) async fn initialize_existing_for_operations(
    runtime_home: &Path,
    app_config: &AppConfig,
) -> Result<Option<SqliteDatabase>> {
    let database_path = gateway_database_path(runtime_home, app_config)?;
    if !database_path.exists() {
        return Ok(None);
    }

    initialize(runtime_home, app_config).await.map(Some)
}

#[cfg(test)]
mod tests {
    use futures_util::StreamExt;
    use pioneer_observability::DatabaseOperation;
    use pioneer_sqlite::{
        normalize_relative_database_file_name, sqlite_connection_url,
        sqlite_read_only_connection_url,
    };
    use sea_orm::{ConnectionTrait, DbBackend, Statement, StreamTrait, TransactionTrait};
    use std::path::Path;

    #[tokio::test]
    async fn installer_inspection_reads_history_without_migrations_or_creation() {
        let temp = tempfile::tempdir().unwrap();
        let config = pioneer_config::AppConfig::load().unwrap();
        let path = super::gateway_database_path(temp.path(), &config).unwrap();
        assert_eq!(
            super::inspect_applied_migration_names(temp.path(), &config)
                .await
                .unwrap(),
            None
        );
        assert!(!path.exists(), "inspection must not create a DB");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let writer = sea_orm::Database::connect(sqlite_connection_url(&path))
            .await
            .unwrap();
        let db = pioneer_sqlite::SqliteDatabase::from_single_connection(writer);
        db.execute_unprepared(
            "CREATE TABLE seaql_migrations (version TEXT PRIMARY KEY, applied_at BIGINT NOT NULL)",
        )
        .await
        .unwrap();
        db.execute_unprepared(
            "INSERT INTO seaql_migrations VALUES ('future_unknown_to_installer', 1)",
        )
        .await
        .unwrap();
        db.close().await.unwrap();
        let before = std::fs::read(&path).unwrap();
        assert_eq!(
            super::inspect_applied_migration_names(temp.path(), &config)
                .await
                .unwrap(),
            Some(vec!["future_unknown_to_installer".into()])
        );
        assert_eq!(
            std::fs::read(&path).unwrap(),
            before,
            "no migrations/history writes"
        );
        // A malformed history is unknown, never an empty compatible set.
        std::fs::write(&path, b"not a SQLite database").unwrap();
        assert!(
            super::inspect_applied_migration_names(temp.path(), &config)
                .await
                .is_err()
        );
    }

    #[test]
    fn builds_read_only_sqlite_connection_url() {
        let path = Path::new("/tmp/gateway.db");
        let url = sqlite_read_only_connection_url(path);
        assert_eq!(url, "sqlite:///tmp/gateway.db?mode=ro");
    }

    #[test]
    fn classifies_sql_without_exporting_statement_text() {
        assert_eq!(
            super::sqlite_operation(" SELECT * FROM thread"),
            DatabaseOperation::Select
        );
        assert_eq!(
            super::sqlite_operation("insert into turn values (?)"),
            DatabaseOperation::Insert
        );
        assert_eq!(
            super::sqlite_operation("BEGIN IMMEDIATE"),
            DatabaseOperation::Transaction
        );
        assert_eq!(
            super::sqlite_operation("PRAGMA journal_mode"),
            DatabaseOperation::Pragma
        );
        assert_eq!(
            super::sqlite_operation("WITH rows AS (...) SELECT 1"),
            DatabaseOperation::Other
        );
    }

    #[test]
    fn fingerprint_cache_is_bounded_and_uses_an_overflow_bucket() {
        let limiter = super::QueryFingerprintLimiter {
            max_raw_query_cache_entries: 4,
            max_fingerprints: 2,
            max_metric_series: 2,
            state: std::sync::Mutex::new(super::QueryFingerprintState::default()),
        };
        let fingerprint = |sql, workload| {
            limiter
                .metric_fingerprint(
                    sql,
                    pioneer_observability::DatabaseRole::Reader,
                    pioneer_observability::DatabaseOperation::Select,
                    workload,
                    false,
                )
                .metric
        };
        let first = fingerprint(
            "SELECT one FROM probe",
            pioneer_observability::DatabaseWorkload::TimelinePage,
        );
        let second = fingerprint(
            "SELECT two FROM probe",
            pioneer_observability::DatabaseWorkload::TimelinePage,
        );

        assert_ne!(first, 0);
        assert_ne!(second, 0);
        assert_eq!(
            fingerprint(
                "SELECT one FROM probe",
                pioneer_observability::DatabaseWorkload::TimelinePage,
            ),
            first
        );
        assert_eq!(
            fingerprint(
                "SELECT three FROM probe",
                pioneer_observability::DatabaseWorkload::TimelinePage,
            ),
            0
        );
        assert_eq!(
            fingerprint(
                "SELECT one FROM probe",
                pioneer_observability::DatabaseWorkload::ThreadTreeLoad,
            ),
            0,
            "a new attribute combination must use overflow after the series cap"
        );
        // Metric-series overflow preserves canonical identity; shape overflow
        // remains unknown, without a second normalization or SQL hash.
        let rejected = limiter.metric_fingerprint(
            "SELECT one FROM probe",
            pioneer_observability::DatabaseRole::Reader,
            pioneer_observability::DatabaseOperation::Select,
            pioneer_observability::DatabaseWorkload::ThreadTreeLoad,
            false,
        );
        assert_eq!(rejected.canonical, Some(first));
        assert_eq!(rejected.metric, 0);
        let unknown = limiter.metric_fingerprint(
            "SELECT three FROM probe",
            pioneer_observability::DatabaseRole::Reader,
            pioneer_observability::DatabaseOperation::Select,
            pioneer_observability::DatabaseWorkload::TimelinePage,
            false,
        );
        assert_eq!(unknown.canonical, None);
        assert_eq!(unknown.metric, 0);
        assert_eq!(
            limiter
                .state
                .lock()
                .expect("fingerprint cache lock")
                .normalized_fingerprints
                .len(),
            2
        );
    }

    #[test]
    fn dynamic_literals_share_one_normalized_fingerprint_and_cache_no_raw_sql() {
        let limiter = super::QueryFingerprintLimiter {
            max_raw_query_cache_entries: 4,
            max_fingerprints: 1,
            max_metric_series: 4,
            state: std::sync::Mutex::new(super::QueryFingerprintState::default()),
        };
        let fingerprint = |sql| {
            limiter
                .metric_fingerprint(
                    sql,
                    pioneer_observability::DatabaseRole::Reader,
                    pioneer_observability::DatabaseOperation::Select,
                    pioneer_observability::DatabaseWorkload::TimelinePage,
                    false,
                )
                .metric
        };

        let first = fingerprint(
            "SELECT payload FROM turn_event WHERE sequence = 42 AND id = 'secret' -- request",
        );
        let second =
            fingerprint(" select payload from turn_event where sequence=9001 and id='different' ");
        assert_ne!(first, 0);
        assert_eq!(second, first);

        let state = limiter.state.lock().expect("fingerprint cache lock");
        assert_eq!(state.normalized_fingerprints.len(), 1);
        assert_eq!(state.raw_query_cache.len(), 2);
        assert!(
            state
                .normalized_fingerprints
                .keys()
                .all(|shape| !shape.contains("42") && !shape.contains("9001"))
        );

        assert_eq!(
            state
                .normalized_fingerprints
                .keys()
                .map(|shape| shape.as_ref())
                .collect::<Vec<_>>(),
            vec!["select payload from turn_event where sequence=? and id=?"]
        );
    }

    #[tokio::test]
    async fn initializes_independent_read_only_reader_and_single_writer() {
        let runtime = tempfile::tempdir().expect("temporary Gateway runtime");
        let mut config = pioneer_config::AppConfig::load().expect("load test config");
        config.gateway.database.file_name = "routing-test.db".to_owned();
        config.gateway.database.max_connections = 4;

        let database = super::initialize(runtime.path(), &config)
            .await
            .expect("initialize split Gateway database");
        assert_eq!(database.reader_max_connections(), 4);
        assert_eq!(database.writer_max_connections(), 1);
        assert!(
            database
                .reader_query_only_enabled()
                .await
                .expect("read query_only state")
        );

        database
            .execute_unprepared(
                "CREATE TABLE routing_probe (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            )
            .await
            .expect("create probe through writer");
        let inserted = database
            .query_one_raw(Statement::from_string(
                DbBackend::Sqlite,
                "INSERT INTO routing_probe(value) VALUES ('ok') RETURNING value".to_owned(),
            ))
            .await
            .expect("route INSERT RETURNING through writer")
            .expect("inserted probe row");
        assert_eq!(inserted.try_get::<String>("", "value").unwrap(), "ok");

        let selected = database
            .query_one_raw(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT value FROM routing_probe".to_owned(),
            ))
            .await
            .expect("query probe through reader")
            .expect("selected probe row");
        assert_eq!(selected.try_get::<String>("", "value").unwrap(), "ok");

        let mut selected_stream = database
            .stream_raw(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT value FROM routing_probe".to_owned(),
            ))
            .await
            .expect("route streaming SELECT through reader");
        let streamed = selected_stream
            .next()
            .await
            .expect("streamed probe row")
            .expect("read streamed probe row");
        assert_eq!(streamed.try_get::<String>("", "value").unwrap(), "ok");
        drop(selected_stream);

        let mut inserted_stream = database
            .stream_raw(Statement::from_string(
                DbBackend::Sqlite,
                "INSERT INTO routing_probe(value) VALUES ('streamed') RETURNING value".to_owned(),
            ))
            .await
            .expect("route streaming INSERT RETURNING through writer");
        let streamed_insert = inserted_stream
            .next()
            .await
            .expect("streamed inserted row")
            .expect("read streamed inserted row");
        assert_eq!(
            streamed_insert.try_get::<String>("", "value").unwrap(),
            "streamed"
        );
        drop(inserted_stream);

        let extension_probe = database
            .query_one_raw(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT length(zstd_compress('reader-extension-probe', 1)) AS value".to_owned(),
            ))
            .await
            .expect("sqlite-zstd must be registered on read connections")
            .expect("sqlite-zstd probe row");
        assert!(extension_probe.try_get::<i64>("", "value").unwrap() > 0);

        let transaction = database.begin().await.expect("begin writer probe");
        transaction
            .execute_unprepared("UPDATE routing_probe SET value = 'uncommitted'")
            .await
            .expect("hold writer with an uncommitted update");
        let transaction_value = transaction
            .query_one_raw(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT value FROM routing_probe".to_owned(),
            ))
            .await
            .expect("read inside write transaction")
            .expect("transaction probe row");
        assert_eq!(
            transaction_value.try_get::<String>("", "value").unwrap(),
            "uncommitted"
        );
        let concurrent_read = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            database.query_one_raw(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT value FROM routing_probe".to_owned(),
            )),
        )
        .await
        .expect("ordinary read must not wait for the occupied writer")
        .expect("query through independent reader")
        .expect("concurrent probe row");
        assert_eq!(
            concurrent_read.try_get::<String>("", "value").unwrap(),
            "ok"
        );
        transaction.rollback().await.expect("rollback writer probe");

        database.close().await.expect("close both Gateway pools");
    }

    // The production Gateway initialization, fingerprint callback, wrappers,
    // and SQLx worker transport are exercised together. This test is NOT_RUN
    // until external acceptance of the entire changed tree.
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    #[tokio::test]
    async fn sql_cpu_capture_measures_workers_and_excludes_consumer_wait() {
        use crate::database::attribution::scope_database_workload_result;
        use pioneer_observability::{DatabaseOperationSite as Site, DatabaseWorkload as Workload};
        use std::time::Duration;
        pioneer_observability::init_tracing(false);
        let runtime = tempfile::tempdir().unwrap();
        let mut config = pioneer_config::AppConfig::load().unwrap();
        config.gateway.database.file_name = "sql-cpu-test.db".into();
        let database = super::initialize(runtime.path(), &config).await.unwrap();
        let session = pioneer_observability::query_cpu::Session::start(
            60,
            pioneer_observability::telemetry_consent_snapshot().1,
        )
        .unwrap();
        let (sum, maximum) = tokio::join!(
            scope_database_workload_result(Workload::ThreadTreeLoad, Site::ThreadTreeLoad,
                database.query_one_raw(Statement::from_string(DbBackend::Sqlite,
                    "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<400000) SELECT sum(x) AS value FROM n"))),
            scope_database_workload_result(Workload::TimelinePage, Site::TimelinePage,
                database.query_one_raw(Statement::from_string(DbBackend::Sqlite,
                    "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<400000) SELECT max(x) AS value FROM n"))),
        );
        assert_eq!(
            sum.unwrap().unwrap().try_get::<i64>("", "value").unwrap(),
            80_000_200_000
        );
        assert_eq!(
            maximum
                .unwrap()
                .unwrap()
                .try_get::<i64>("", "value")
                .unwrap(),
            400_000
        );
        scope_database_workload_result(Workload::SkillsWatch, Site::SkillsWatch, async {
            let mut stream = database.stream_raw(Statement::from_string(DbBackend::Sqlite,
                "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<1000000) SELECT x FROM n"))
                .await?;
            assert!(stream.next().await.transpose()?.is_some());
            // Deliberately stalled consumer: SQLx blocks on its bounded row
            // channel. This wait must not be charged as SQLite worker CPU.
            tokio::time::sleep(Duration::from_millis(300)).await;
            drop(stream);
            Ok::<_, sea_orm::DbErr>(())
        }).await.unwrap();
        database.close().await.unwrap(); // drain physical worker references
        let snapshot = session.finish();
        assert_eq!(snapshot.coverage.clock_api_errors, 0);
        assert_eq!(snapshot.coverage.clock_regressions, 0);
        assert_eq!(snapshot.coverage.scopes_without_callback, 0);
        assert!(snapshot.coverage.measured_segments >= 3);
        assert!(snapshot.coverage.cpu_clock_reads <= 40_000);
        let json = serde_json::to_value(&snapshot).unwrap();
        let groups = json["groups"].as_array().unwrap();
        let group = |site: &str| {
            groups
                .iter()
                .find(|g| g["site"].as_str() == Some(site))
                .unwrap()
        };
        let sum = group("thread_tree_load");
        let maximum = group("timeline_page");
        let waiting = group("skills_watch");
        assert!(sum["fingerprint"].is_string());
        assert!(maximum["fingerprint"].is_string());
        assert_ne!(sum["fingerprint"], maximum["fingerprint"]);
        for completed in [sum, maximum] {
            assert_eq!(completed["completed_calls"], 1);
            assert!(completed["cpu_ns"].as_u64().unwrap() > 0);
        }
        assert_eq!(waiting["cancelled"], 1);
        assert!(
            waiting["cpu_ns"].as_u64().unwrap() < 150_000_000,
            "a 300 ms blocked row consumer must not be charged as await duration"
        );
        assert_eq!(snapshot.measured_unknown.cpu_ns, None);
    }

    #[test]
    fn reserves_four_reader_connections_even_for_legacy_single_connection_config() {
        let mut config = pioneer_config::AppConfig::load().expect("load test config");
        config.gateway.database.max_connections = 1;

        let runtime = super::GatewayDatabaseRuntimeConfig::from_app_config(&config)
            .expect("normalize database config");

        assert_eq!(runtime.max_connections, super::MIN_READER_CONNECTIONS);
    }

    #[test]
    fn sqlite_connection_urls_preserve_path_forms() {
        for (path, expected) in [
            ("/tmp/gateway.db", "sqlite:///tmp/gateway.db?mode=rwc"),
            (
                r"C:\Users\alex\gateway.db",
                "sqlite:///C:/Users/alex/gateway.db?mode=rwc",
            ),
        ] {
            let path = Path::new(path);
            let url = sqlite_connection_url(path);
            assert_eq!(url, expected);
        }
    }

    #[test]
    fn relative_database_file_names_reject_invalid_inputs() {
        for (input, rejection_message, expected) in [
            (
                "   ",
                "must reject empty file name",
                "gateway.database.file_name must not be empty",
            ),
            (
                "/tmp/gateway.db",
                "must reject absolute file name",
                "gateway.database.file_name must be relative",
            ),
        ] {
            let error = normalize_relative_database_file_name(input, "gateway.database.file_name")
                .expect_err(rejection_message);
            assert!(
                format!("{error:#}").contains(expected),
                "unexpected error: {error:#}"
            );
        }
    }
}
