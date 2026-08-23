use anyhow::{Context, Result, bail};
use migration::{Migrator, MigratorTrait};
use pioneer_config::AppConfig;
use pioneer_sqlite::{
    DEFAULT_LOCK_RETRY_ATTEMPTS, DEFAULT_LOCK_RETRY_BASE_DELAY_MS, apply_sqlite_pragmas,
    is_anyhow_sqlite_lock, normalize_relative_database_file_name, retry_with_backoff,
    sqlite_connection_url,
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
            max_connections: database.max_connections,
            connect_timeout: Duration::from_millis(database.connect_timeout_ms),
            acquire_timeout: Duration::from_millis(database.acquire_timeout_ms),
            idle_timeout: Duration::from_millis(database.idle_timeout_ms),
            sqlx_logging: database.sqlx_logging,
        })
    }
}

pub async fn initialize(runtime_home: &Path, app_config: &AppConfig) -> Result<DatabaseConnection> {
    initialize_inner(runtime_home, app_config, None).await
}

pub async fn initialize_with_startup(
    runtime_home: &Path,
    app_config: &AppConfig,
    startup: &pioneer_observability::GatewayStartupTrace,
) -> Result<DatabaseConnection> {
    initialize_inner(runtime_home, app_config, Some(startup)).await
}

async fn initialize_inner(
    runtime_home: &Path,
    app_config: &AppConfig,
    startup: Option<&pioneer_observability::GatewayStartupTrace>,
) -> Result<DatabaseConnection> {
    let database_open_stage =
        startup.map(|trace| trace.stage(pioneer_observability::GatewayStartupStage::DatabaseOpen));
    let config = GatewayDatabaseRuntimeConfig::from_app_config(app_config)?;
    pioneer_sqlite::zstd::register_auto_extension_once()
        .context("failed to register sqlite-zstd gateway database extension")?;

    let database_path = runtime_home.join(config.file_name.as_str());
    let database_url = sqlite_connection_url(database_path.as_path());

    let mut options = ConnectOptions::new(database_url.clone());
    options.max_connections(config.max_connections);
    options.connect_timeout(config.connect_timeout);
    options.acquire_timeout(config.acquire_timeout);
    options.idle_timeout(config.idle_timeout);
    options.sqlx_logging(config.sqlx_logging);
    // SQLx measures the complete wait for a pool permit/connection. A TRACE
    // event keeps normal logs quiet and is consumed by the observability layer.
    options.map_sqlx_sqlite_pool_opts(|pool_options| {
        pool_options.acquire_time_level(log::LevelFilter::Trace)
    });

    let mut connection = Database::connect(options)
        .await
        .with_context(|| format!("failed to connect to gateway database `{database_url}`"))?;
    if let Some(stage) = database_open_stage {
        stage.succeed();
    }

    let database_configure_stage = startup
        .map(|trace| trace.stage(pioneer_observability::GatewayStartupStage::DatabaseConfigure));
    connection.set_metric_callback(|info| {
        pioneer_observability::record_database_operation(
            pioneer_observability::DatabaseRole::Shared,
            sqlite_operation(info.statement.sql.as_str()),
            info.elapsed,
            info.failed,
        );
    });
    let pool = connection.get_sqlite_connection_pool().clone();
    pioneer_observability::register_database_pool_observer(
        pioneer_observability::DatabaseRole::Shared,
        u64::from(config.max_connections),
        move || pioneer_observability::DatabasePoolSnapshot {
            size: u64::from(pool.size()),
            idle: u64::try_from(pool.num_idle()).unwrap_or(u64::MAX),
        },
    );

    connection
        .ping()
        .await
        .context("failed to ping gateway database")?;

    retry_with_backoff(
        || apply_sqlite_pragmas(&connection),
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
            Migrator::up(&connection, None)
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

    info!(
        database_path = %database_path.display(),
        "gateway database is ready"
    );

    Ok(connection)
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
) -> Result<Option<DatabaseConnection>> {
    let database_path = gateway_database_path(runtime_home, app_config)?;
    if !database_path.exists() {
        return Ok(None);
    }

    initialize(runtime_home, app_config).await.map(Some)
}

#[cfg(test)]
mod tests {
    use pioneer_observability::DatabaseOperation;
    use pioneer_sqlite::{normalize_relative_database_file_name, sqlite_connection_url};
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
}
