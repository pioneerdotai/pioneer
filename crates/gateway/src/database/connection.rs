use anyhow::{Context, Result};
use migration::Migrator;
use pioneer_config::AppConfig;
use pioneer_postgres::BundledPostgres;
use pioneer_postgres_pools::{PostgresDatabase, PostgresWriteExecutor};
use sea_orm::{ConnectOptions, Database, DatabaseConnection};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
#[path = "query_fingerprint.rs"]
mod query_fingerprint;
use query_fingerprint::QueryFingerprintLimiter;
#[path = "query_observer.rs"]
pub(super) mod query_observer;
#[cfg(test)]
use query_fingerprint::QueryFingerprintState;
#[cfg(test)]
use query_observer::classify_operation;

/// Check the persisted migration history through a scoped read-only connection.
pub(crate) async fn inspect_applied_migration_names(
    runtime_home: &Path,
    app_config: &AppConfig,
) -> Result<Option<Vec<String>>> {
    with_existing_database(
        runtime_home,
        app_config,
        OperatorAccess::ReadOnly,
        async |database, _| {
            pioneer_crud::CrudStore::new(database)
                .with_maintenance_access()
                .inspect_applied_migration_names()
                .await
        },
    )
    .await
}

pub async fn initialize_with_startup(
    runtime_home: &Path,
    app_config: &AppConfig,
    startup: &pioneer_observability::GatewayStartupTrace,
) -> Result<(PostgresDatabase, Option<BundledPostgres>)> {
    use pioneer_observability::{DatabaseRole, GatewayStartupStage};
    let database_config = &app_config.gateway.database;
    ensure_postgres_pool_config(database_config)?;
    let stage = startup.stage(GatewayStartupStage::DatabaseOpen);
    let capacity = database_config.max_connections.max(4);
    let postgres;
    let (mut writer_options, mut reader_options) =
        if let Some(options) = external_options(runtime_home, app_config, capacity)? {
            postgres = None;
            options
        } else {
            let instance = BundledPostgres::start(runtime_home).await?;
            let writer = instance.connect_options(capacity);
            let reader = instance.reader_options(capacity);
            postgres = Some(instance);
            (writer, reader)
        };
    stage.succeed();
    let prepared = async {
        for (options, name, read_only) in [
            (&mut writer_options, "pioneer_gateway_writer", false),
            (&mut reader_options, "pioneer_gateway_reader", true),
        ] {
            options
                .connect_timeout(Duration::from_millis(database_config.connect_timeout_ms))
                .acquire_timeout(Duration::from_millis(database_config.acquire_timeout_ms))
                .idle_timeout(Duration::from_millis(database_config.idle_timeout_ms))
                .sqlx_logging(false)
                .map_sqlx_postgres_opts(move |session| {
                    let session = session.application_name(name);
                    if read_only {
                        session.options([("default_transaction_read_only", "on")])
                    } else {
                        session
                    }
                })
                .map_sqlx_postgres_pool_opts(|pool| {
                    pool.acquire_time_level("trace".parse().expect("valid SQLx timing level"))
                });
        }
        let mut writer = Database::connect(writer_options)
            .await
            .map_err(|_| anyhow::anyhow!("open Gateway PostgreSQL writer pool"))?;
        if postgres.is_none() {
            use migration::MigratorTrait;
            let stage = startup.stage(GatewayStartupStage::DatabaseMigrate);
            if let Err(error) = Migrator::up(&writer, None).await {
                let _ = writer.close().await;
                return Err(error).context("apply Gateway PostgreSQL migrations");
            }
            stage.succeed();
        }
        let mut reader = match Database::connect(reader_options).await {
            Ok(reader) => reader,
            Err(_) => {
                let _ = writer.close().await;
                anyhow::bail!("open Gateway PostgreSQL reader pool");
            }
        };
        let fingerprints = Arc::new(QueryFingerprintLimiter::default());
        configure_observability(
            &mut writer,
            DatabaseRole::Writer,
            capacity,
            fingerprints.clone(),
        );
        configure_observability(&mut reader, DatabaseRole::Reader, capacity, fingerprints);
        let executor = PostgresWriteExecutor::with_observer(writer, super::write_observer());
        let database = PostgresDatabase::from_executor_with_read_observer(
            reader,
            executor,
            super::read_observer(),
        );
        if let Err(error) = database.maintenance().validate_reader().await {
            let _ = database.close().await;
            return Err(error).context("validate Gateway PostgreSQL read pool");
        }
        Ok(database)
    }
    .await;
    match prepared {
        Ok(database) => Ok((database, postgres)),
        Err(error) => {
            if let Some(postgres) = postgres {
                let _ = postgres.stop().await;
            }
            Err(error)
        }
    }
}

fn external_options(
    runtime_home: &Path,
    config: &AppConfig,
    capacity: u32,
) -> Result<Option<(ConnectOptions, ConnectOptions)>> {
    let database_config = &config.gateway.database;
    let (url, source) = if let Some(url) = &database_config.development_url {
        anyhow::ensure!(
            pioneer_config::RuntimeEnvironment::current()
                == pioneer_config::RuntimeEnvironment::Development,
            "gateway.database.development_url is only available in development"
        );
        (url.as_str().to_owned(), "gateway.database.development_url")
    } else {
        let Some(url) = external_database_url(runtime_home)? else {
            return Ok(None);
        };
        (url, "DATABASE_URL")
    };
    let writer = external_writer_options(&url, source, capacity, database_config)?;
    let mut reader = writer.clone();
    reader.map_sqlx_postgres_opts(|options| {
        options.options([("default_transaction_read_only", "on")])
    });
    Ok(Some((writer, reader)))
}

fn external_database_url(runtime_home: &Path) -> Result<Option<String>> {
    match std::env::var("DATABASE_URL") {
        Ok(url) => return Ok(Some(url)),
        Err(std::env::VarError::NotPresent) => {}
        Err(std::env::VarError::NotUnicode(_)) => anyhow::bail!("DATABASE_URL must be UTF-8"),
    }

    let entries = match dotenvy::from_path_iter(runtime_home.join(".env")) {
        Ok(entries) => entries,
        Err(error) if error.not_found() => return Ok(None),
        Err(_) => anyhow::bail!("open Gateway runtime .env"),
    };
    for entry in entries {
        // dotenvy syntax errors include the original line and can expose credentials.
        let (key, value) = entry.map_err(|_| anyhow::anyhow!("parse Gateway runtime .env"))?;
        if key == "DATABASE_URL" {
            return Ok(Some(value));
        }
    }
    Ok(None)
}

fn external_writer_options(
    url: &str,
    source: &str,
    capacity: u32,
    config: &pioneer_config::GatewayDatabaseConfig,
) -> Result<ConnectOptions> {
    let parsed = url::Url::parse(url)
        .map_err(|_| anyhow::anyhow!("{source} must be a valid PostgreSQL URL"))?;
    let session = url
        .parse::<sea_orm::sqlx::postgres::PgConnectOptions>()
        .map_err(|_| anyhow::anyhow!("{source} must be a valid PostgreSQL URL"))?;
    anyhow::ensure!(
        matches!(parsed.scheme(), "postgres" | "postgresql")
            && !parsed.username().is_empty()
            && !parsed.path().trim_start_matches('/').is_empty()
            && !session.get_username().is_empty()
            && session.get_database().is_some_and(|database| {
                !matches!(database, "" | "postgres" | "template0" | "template1")
            }),
        "{source} must select an explicit role and a PostgreSQL application database"
    );

    let mut writer = ConnectOptions::new(parsed.to_string());
    writer
        .min_connections(0)
        .max_connections(capacity)
        .connect_timeout(Duration::from_millis(config.connect_timeout_ms))
        .acquire_timeout(Duration::from_millis(config.acquire_timeout_ms))
        .idle_timeout(Duration::from_millis(config.idle_timeout_ms))
        .sqlx_logging(false);
    Ok(writer)
}

fn ensure_postgres_pool_config(database: &pioneer_config::GatewayDatabaseConfig) -> Result<()> {
    for (name, value) in [
        ("max_connections", u64::from(database.max_connections)),
        ("connect_timeout_ms", database.connect_timeout_ms),
        ("acquire_timeout_ms", database.acquire_timeout_ms),
        ("idle_timeout_ms", database.idle_timeout_ms),
    ] {
        anyhow::ensure!(value > 0, "gateway.database.{name} must be greater than 0");
    }
    Ok(())
}

fn configure_observability(
    connection: &mut DatabaseConnection,
    role: pioneer_observability::DatabaseRole,
    max_connections: u32,
    query_fingerprints: Arc<QueryFingerprintLimiter>,
) {
    let observer = query_observer::install_query_observer(connection, role, query_fingerprints);
    pioneer_observability::register_database_pool_observer(
        role,
        u64::from(max_connections),
        observer,
    );
}

/// Coordinates Gateway and offline writers that share a runtime home.
pub(crate) fn lock_gateway_database_owner(runtime_home: &Path) -> Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    let owner = options
        .open(runtime_home.join("gateway-database.lock"))
        .context("open Gateway database ownership lock")?;
    match fs4::FileExt::try_lock(&owner) {
        Ok(()) => Ok(owner),
        Err(fs4::TryLockError::WouldBlock) => Err(pioneer_postgres::ClusterBusy.into()),
        Err(error) => Err(error).context("lock Gateway database ownership"),
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum OperatorAccess {
    ReadOnly,
    Write,
}

pub(crate) async fn with_existing_database<T>(
    runtime_home: &Path,
    app_config: &AppConfig,
    access: OperatorAccess,
    operation: impl AsyncFnOnce(PostgresDatabase, Option<PathBuf>) -> Result<T>,
) -> Result<Option<T>> {
    let mut owned = None;
    let prepared = async {
        let (writer, reader, data_directory, _external_owner) = if let Some((writer, reader)) =
            external_options(runtime_home, app_config, 2)?
        {
            let owner = if access == OperatorAccess::Write {
                Some(lock_gateway_database_owner(runtime_home)?)
            } else {
                None
            };
            let (writer, reader) = operator_pools(writer, reader, access).await?;
            (writer, reader, None, owner)
        } else {
            let directory = pioneer_postgres::data_directory(runtime_home);
            match std::fs::metadata(directory.join("PG_VERSION")) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(error) => return Err(error).context("inspect Gateway PostgreSQL data"),
                Ok(version) => {
                    anyhow::ensure!(version.is_file(), "invalid PostgreSQL data version")
                }
            }
            let installed = pioneer_config::load_install_state(&app_config.install_state_path()?)?;
            let executable = match installed {
                Some(installed) => installed.binary_path,
                None => std::env::current_exe().context("locate Gateway executable")?,
            };
            let (writer, reader) =
                match BundledPostgres::start_existing(runtime_home, &executable).await {
                    Ok(instance) => {
                        let writer = instance.connect_options(2);
                        let reader = instance.reader_options(2);
                        owned = Some(instance);
                        operator_pools(writer, reader, access).await?
                    }
                    Err(error)
                        if access == OperatorAccess::ReadOnly
                            && error
                                .downcast_ref::<pioneer_postgres::ClusterBusy>()
                                .is_some() =>
                    {
                        let reader = BundledPostgres::running_reader(runtime_home).await?;
                        (reader.clone(), reader)
                    }
                    Err(error) => return Err(error),
                };
            (writer, reader, Some(directory), None)
        };
        let database = PostgresDatabase::from_executor_with_read_observer(
            reader,
            PostgresWriteExecutor::with_observer(writer, super::write_observer()),
            super::read_observer(),
        )
        .maintenance();
        let result = operation(database.clone(), data_directory).await;
        let closed = database.close().await;
        let result = result?;
        closed.context("close Gateway operator pools")?;
        Ok(Some(result))
    }
    .await;
    let stopped = match owned {
        Some(instance) => instance.stop().await,
        None => Ok(()),
    };
    let result = prepared?;
    stopped.context("stop offline operator PostgreSQL")?;
    Ok(result)
}

async fn operator_pools(
    writer: ConnectOptions,
    reader: ConnectOptions,
    access: OperatorAccess,
) -> Result<(DatabaseConnection, DatabaseConnection)> {
    let reader = Database::connect(reader)
        .await
        .map_err(|_| anyhow::anyhow!("open Gateway operator PostgreSQL reader"))?;
    if access == OperatorAccess::ReadOnly {
        return Ok((reader.clone(), reader));
    }
    let writer = Database::connect(writer).await;
    match writer {
        Ok(writer) => Ok((writer, reader)),
        Err(_) => {
            let _ = reader.close().await;
            anyhow::bail!("open Gateway operator PostgreSQL writer")
        }
    }
}

#[cfg(test)]
mod tests {
    use futures_util::StreamExt;
    use migration::{Migrator, MigratorTrait};
    use pioneer_observability::DatabaseOperation;
    use sea_orm::{ConnectionTrait, DbBackend, Statement, StreamTrait, TransactionTrait};

    fn development_config(url: &url::Url) -> anyhow::Result<pioneer_config::AppConfig> {
        let mut config = pioneer_config::AppConfig::load()?;
        config.gateway.database.development_url = Some(serde_json::from_value(
            serde_json::Value::String(url.to_string()),
        )?);
        Ok(config)
    }

    #[tokio::test]
    async fn gateway_postgres_startup_migrates_empty_database_and_recovers_committed_rows()
    -> anyhow::Result<()> {
        pioneer_postgres_test::with_empty_database(async |fixture| {
            let config = development_config(fixture.url())?;
            let home = tempfile::tempdir()?;
            let trace = pioneer_observability::GatewayStartupTrace::start();

            let (first, process) =
                super::initialize_with_startup(home.path(), &config, &trace).await?;
            assert!(process.is_none(), "development owns only its two pools");
            assert_eq!(
                ConnectionTrait::get_database_backend(&first),
                DbBackend::Postgres
            );
            first.maintenance().validate_reader().await?;

            let writer = first.begin().await?;
            writer
                .execute_unprepared(
                    "INSERT INTO workspace (id, name) VALUES ('persisted', 'After restart')",
                )
                .await?;
            writer.commit().await?;
            let result = first
                .query_one_raw(Statement::from_string(
                    DbBackend::Postgres,
                    "SELECT name FROM workspace WHERE id = 'persisted'",
                ))
                .await?
                .expect("committed workspace");
            assert_eq!(result.try_get::<String>("", "name")?, "After restart");

            let history = super::inspect_applied_migration_names(home.path(), &config)
                .await?
                .expect("development database has migration history");
            let expected_history = Migrator::migrations()
                .iter()
                .map(|migration| migration.name().to_owned())
                .collect::<Vec<_>>();
            assert_eq!(history, expected_history);
            first.close().await?;

            let (restarted, process) =
                super::initialize_with_startup(home.path(), &config, &trace).await?;
            assert!(process.is_none());
            let result = restarted
                .query_one_raw(Statement::from_string(
                    DbBackend::Postgres,
                    "SELECT name FROM workspace WHERE id = 'persisted'",
                ))
                .await?
                .expect("committed workspace after restart");
            assert_eq!(result.try_get::<String>("", "name")?, "After restart");
            assert_eq!(
                super::inspect_applied_migration_names(home.path(), &config).await?,
                Some(history),
            );
            restarted.close().await?;
            assert!(!home.path().join("postgres").exists());
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn gateway_runtime_dotenv_selects_external_postgres_for_startup_and_inspection()
    -> anyhow::Result<()> {
        pioneer_postgres_test::with_empty_database(async |fixture| {
            let home = tempfile::tempdir()?;
            std::fs::write(
                home.path().join(".env"),
                format!("DATABASE_URL={}\n", fixture.url()),
            )?;
            let mut config = pioneer_config::AppConfig::load()?;
            config.gateway.database.development_url = None;
            let trace = pioneer_observability::GatewayStartupTrace::start();
            let owner = super::lock_gateway_database_owner(home.path())?;

            let (database, bundled) =
                super::initialize_with_startup(home.path(), &config, &trace).await?;
            assert!(bundled.is_none());
            let transaction = database.begin().await?;
            transaction
                .execute_unprepared(
                    "INSERT INTO workspace (id, name) VALUES ('external', 'External database')",
                )
                .await?;
            transaction.commit().await?;

            let busy = super::with_existing_database(
                home.path(),
                &config,
                super::OperatorAccess::Write,
                async |_, _| Ok(()),
            )
            .await
            .unwrap_err();
            assert!(busy.is::<pioneer_postgres::ClusterBusy>());

            let migrations = super::inspect_applied_migration_names(home.path(), &config)
                .await?
                .expect("external database migration history");
            assert_eq!(migrations.len(), Migrator::migrations().len());
            database.close().await?;
            drop(owner);
            let _owner = super::lock_gateway_database_owner(home.path())?;

            let (restarted, bundled) =
                super::initialize_with_startup(home.path(), &config, &trace).await?;
            assert!(bundled.is_none());
            assert!(restarted.reader_is_read_only().await?);
            let workspace = restarted
                .query_one_raw(Statement::from_string(
                    DbBackend::Postgres,
                    "SELECT name FROM workspace WHERE id = 'external'",
                ))
                .await?
                .expect("workspace persisted across restart");
            assert_eq!(
                workspace.try_get::<String>("", "name")?,
                "External database"
            );
            restarted.close().await?;
            assert!(!home.path().join("postgres").exists());
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn installer_inspection_reads_postgres_history_without_applying_migrations()
    -> anyhow::Result<()> {
        pioneer_postgres_test::with_empty_database(async |fixture| {
            let home = tempfile::tempdir()?;
            let mut bundled = pioneer_config::AppConfig::load()?;
            bundled.gateway.database.development_url = None;
            assert_eq!(
                super::inspect_applied_migration_names(home.path(), &bundled).await?,
                None,
            );
            assert!(!home.path().join("postgres").exists());
            let config = development_config(fixture.url())?;
            assert!(
                super::inspect_applied_migration_names(home.path(), &config).await.is_err(),
                "a missing migration table is unknown"
            );
            let db = fixture.connection();
            db.execute_unprepared(
                "CREATE TABLE seaql_migrations (version TEXT PRIMARY KEY, applied_at BIGINT NOT NULL); INSERT INTO seaql_migrations VALUES ('future_unknown_to_installer', 1);"
            ).await?;
            assert_eq!(
                super::inspect_applied_migration_names(home.path(), &config).await?,
                Some(vec!["future_unknown_to_installer".into()]),
            );
            assert!(db.query_one_raw(Statement::from_string(
                DbBackend::Postgres, "SELECT 1 FROM pg_class WHERE relname = 'workspace'",
            )).await?.is_none());
            db.execute_unprepared("DROP TABLE seaql_migrations; CREATE TABLE seaql_migrations (broken TEXT)").await?;
            assert!(super::inspect_applied_migration_names(home.path(), &config).await.is_err());
            Ok(())
        }).await
    }

    #[test]
    fn classifies_sql_without_exporting_statement_text() {
        assert_eq!(
            super::classify_operation(" SELECT * FROM thread"),
            DatabaseOperation::Select
        );
        assert_eq!(
            super::classify_operation("insert into turn values ($1)"),
            DatabaseOperation::Insert
        );
        assert_eq!(
            super::classify_operation("BEGIN"),
            DatabaseOperation::Transaction
        );
        assert_eq!(
            super::classify_operation("EXPLAIN SELECT 1"),
            DatabaseOperation::Select
        );
        assert_eq!(
            super::classify_operation("WITH rows AS (...) SELECT 1"),
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

    // Unlike explicit close, bootstrap error/cancellation and discarded task
    // output must release ownership through pool Drop with metrics registered.
    #[tokio::test]
    async fn observed_bootstrap_pool_drops_on_error_cancel_and_discard() {
        pioneer_observability::init_otlp_observability(
            pioneer_observability::OtlpTelemetryConfig {
                metrics_endpoint: "http://127.0.0.1:9/v1/metrics".into(),
                traces_endpoint: "http://127.0.0.1:9/v1/traces".into(),
                export_interval: std::time::Duration::from_secs(900),
                export_timeout: std::time::Duration::from_millis(100),
                deployment_environment: None,
                service_version: None,
            },
        )
        .unwrap();
        assert!(
            pioneer_observability::register_database_pool_observer(
                pioneer_observability::DatabaseRole::Writer,
                1,
                || pioneer_observability::DatabasePoolSnapshot { size: 0, idle: 0 },
            ),
            "the regression requires an active observer registry"
        );
        for mode in 0..3 {
            pioneer_postgres_test::with_database(async |fixture| {
                let runtime = tempfile::tempdir()?;
                let config = development_config(fixture.url())?;
                let home = runtime.path().to_owned();
                let (ready, initialized) = tokio::sync::oneshot::channel();
                let (release, released) = tokio::sync::oneshot::channel();
                let task = tokio::spawn(async move {
                    let trace = pioneer_observability::GatewayStartupTrace::start();
                    let (database, process) =
                        super::initialize_with_startup(&home, &config, &trace).await?;
                    assert!(process.is_none());
                    ready.send(()).unwrap();
                    let _ = released.await;
                    if mode == 0 {
                        anyhow::bail!("bootstrap caller rejected initialized pools");
                    }
                    Ok::<_, anyhow::Error>(database)
                });
                if initialized.await.is_err() {
                    task.await??;
                    anyhow::bail!("bootstrap task exited before reporting initialized pools");
                }
                async fn extra_connections(
                    db: &sea_orm::DatabaseConnection,
                ) -> anyhow::Result<i64> {
                    db.query_one_raw(Statement::from_string(
                        DbBackend::Postgres,
                        "SELECT count(*) AS connections FROM pg_stat_activity
                         WHERE datname = current_database()
                           AND application_name IN ('pioneer_gateway_writer','pioneer_gateway_reader')",
                    ))
                    .await?
                    .expect("database connection count")
                    .try_get("", "connections")
                    .map_err(Into::into)
                }
                assert!(extra_connections(fixture.connection()).await? >= 2);
                match mode {
                    0 => {
                        release.send(()).unwrap();
                        assert!(task.await?.is_err());
                    }
                    1 => {
                        task.abort();
                        assert!(task.await.unwrap_err().is_cancelled());
                        drop(release);
                    }
                    _ => {
                        drop(task);
                        release.send(()).unwrap();
                    }
                }
                tokio::time::timeout(std::time::Duration::from_secs(10), async {
                    while extra_connections(fixture.connection()).await? != 0 {
                        tokio::task::yield_now().await;
                    }
                    Ok::<_, anyhow::Error>(())
                })
                .await??;
                Ok(())
            })
            .await
            .unwrap();
        }
    }

    #[tokio::test]
    async fn initializes_independent_read_only_reader_and_concurrent_writers() -> anyhow::Result<()>
    {
        pioneer_postgres_test::with_database(async |fixture| {
            let runtime = tempfile::tempdir()?;
            let mut config = development_config(fixture.url())?;
            config.gateway.database.max_connections = 1;
            let trace = pioneer_observability::GatewayStartupTrace::start();
            let (database, process) =
                super::initialize_with_startup(runtime.path(), &config, &trace).await?;
            assert!(process.is_none());
            assert_eq!(database.reader_max_connections(), 4);
            assert_eq!(database.writer_max_connections(), 4);
            assert!(database.reader_is_read_only().await?);
            database.execute_unprepared(
                "CREATE TABLE routing_probe (id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY, value text NOT NULL)"
            ).await?;

            let insert = |value| Statement::from_sql_and_values(
                DbBackend::Postgres,
                "INSERT INTO routing_probe(value) VALUES ($1) RETURNING value",
                [sea_orm::Value::from(value)],
            );
            let inserted = database.query_one_raw(insert("ok")).await?.expect("inserted row");
            assert_eq!(inserted.try_get::<String>("", "value")?, "ok");
            let select = || Statement::from_string(
                DbBackend::Postgres, "SELECT value FROM routing_probe WHERE id = 1"
            );
            let mut selected = database.stream_raw(select()).await?;
            let row = selected.next().await.expect("streamed row")?;
            assert_eq!(row.try_get::<String>("", "value")?, "ok");
            drop(selected);
            let mut inserted = database.stream_raw(insert("streamed")).await?;
            let row = inserted.next().await.expect("streamed insertion")?;
            assert_eq!(row.try_get::<String>("", "value")?, "streamed");
            drop(inserted);

            let readonly = database.begin_read().await?;
            let pgvector = readonly.query_one_raw(Statement::from_string(
                DbBackend::Postgres, "SELECT '[3,4]'::vector <-> '[0,0]'::vector AS distance"
            )).await?.expect("vector distance");
            assert_eq!(pgvector.try_get::<f64>("", "distance")?, 5.0);
            assert!(readonly.execute_unprepared("UPDATE routing_probe SET value = 'rejected'").await.is_err());
            readonly.rollback().await?;

            let transaction = database.begin().await?;
            transaction.execute_unprepared("UPDATE routing_probe SET value = 'uncommitted' WHERE id = 1").await?;
            let row = transaction.query_one_raw(select()).await?.expect("provisional update");
            assert_eq!(row.try_get::<String>("", "value")?, "uncommitted");
            let row = tokio::time::timeout(
                std::time::Duration::from_secs(2), database.query_one_raw(select())
            ).await??.expect("concurrent read");
            assert_eq!(row.try_get::<String>("", "value")?, "ok");
            let other_writer = database.begin().await?;
            let row = other_writer.query_one_raw(insert("other writer")).await?.expect("other writer inserted");
            assert_eq!(row.try_get::<String>("", "value")?, "other writer");
            other_writer.commit().await?;
            transaction.rollback().await?;
            database.close().await?;
            Ok(())
        }).await
    }
}
