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

    let connection = Database::connect(options)
        .await
        .with_context(|| format!("failed to connect to gateway database `{database_url}`"))?;

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

    info!(
        database_path = %database_path.display(),
        "gateway database is ready"
    );

    Ok(connection)
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
}
