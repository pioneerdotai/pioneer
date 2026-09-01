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
    let writer_url = sqlite_connection_url(database_path.as_path());

    let mut writer = Database::connect(connect_options(
        writer_url.clone(),
        WRITER_MAX_CONNECTIONS,
        &config,
    ))
    .await
    .with_context(|| format!("failed to connect to gateway writer `{writer_url}`"))?;
    if let Some(stage) = database_open_stage {
        stage.succeed();
    }

    let database_configure_stage = startup
        .map(|trace| trace.stage(pioneer_observability::GatewayStartupStage::DatabaseConfigure));
    configure_observability(
        &mut writer,
        pioneer_observability::DatabaseRole::Writer,
        WRITER_MAX_CONNECTIONS,
    );

    writer
        .ping()
        .await
        .context("failed to ping gateway writer")?;
    let writer = SqliteWriteExecutor::with_observer(writer, super::write_observer());
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
    let mut reader_options = connect_options(reader_url.clone(), config.max_connections, &config);
    reader_options.map_sqlx_sqlite_opts(|options| {
        options
            .read_only(true)
            .create_if_missing(false)
            .pragma("query_only", "ON")
    });
    let mut reader = Database::connect(reader_options)
        .await
        .with_context(|| format!("failed to connect to gateway read pool `{reader_url}`"))?;
    configure_observability(
        &mut reader,
        pioneer_observability::DatabaseRole::Reader,
        config.max_connections,
    );
    reader
        .ping()
        .await
        .context("failed to ping gateway read pool")?;

    info!(
        database_path = %database_path.display(),
        reader_connections = config.max_connections,
        writer_connections = WRITER_MAX_CONNECTIONS,
        "gateway database is ready"
    );

    Ok(SqliteDatabase::from_executor(reader, writer))
}

fn connect_options(
    database_url: String,
    max_connections: u32,
    config: &GatewayDatabaseRuntimeConfig,
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
    options.map_sqlx_sqlite_opts(|sqlite_options| {
        sqlite_options.busy_timeout(Duration::from_millis(DEFAULT_SQLITE_BUSY_TIMEOUT_MS))
    });
    options
}

fn configure_observability(
    connection: &mut DatabaseConnection,
    role: pioneer_observability::DatabaseRole,
    max_connections: u32,
) {
    connection.set_metric_callback(move |info| {
        pioneer_observability::record_database_operation(
            role,
            sqlite_operation(info.statement.sql.as_str()),
            info.elapsed,
            info.failed,
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

    #[test]
    fn rejects_empty_database_file_name() {
        let error = normalize_relative_database_file_name("   ", "gateway.database.file_name")
            .expect_err("must reject empty file name");
        assert!(
            format!("{error:#}").contains("gateway.database.file_name must not be empty"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn rejects_absolute_database_file_name() {
        let error =
            normalize_relative_database_file_name("/tmp/gateway.db", "gateway.database.file_name")
                .expect_err("must reject absolute file name");
        assert!(
            format!("{error:#}").contains("gateway.database.file_name must be relative"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn builds_sqlite_connection_url() {
        let path = Path::new("/tmp/gateway.db");
        let url = sqlite_connection_url(path);
        assert_eq!(url, "sqlite:///tmp/gateway.db?mode=rwc");
    }

    #[test]
    fn builds_sqlite_connection_url_with_windows_drive_path() {
        let path = Path::new(r"C:\Users\alex\gateway.db");
        let url = sqlite_connection_url(path);
        assert_eq!(url, "sqlite:///C:/Users/alex/gateway.db?mode=rwc");
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

    #[test]
    fn reserves_four_reader_connections_even_for_legacy_single_connection_config() {
        let mut config = pioneer_config::AppConfig::load().expect("load test config");
        config.gateway.database.max_connections = 1;

        let runtime = super::GatewayDatabaseRuntimeConfig::from_app_config(&config)
            .expect("normalize database config");

        assert_eq!(runtime.max_connections, super::MIN_READER_CONNECTIONS);
    }
}
