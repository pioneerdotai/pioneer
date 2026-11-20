//! Isolated databases for PostgreSQL integration tests.

use std::{
    env,
    future::Future,
    ops::AsyncFnOnce,
    panic::{AssertUnwindSafe, resume_unwind},
    sync::Mutex,
    time::Duration,
};

use anyhow::{Context, Result, anyhow, ensure};
use futures_util::FutureExt;
use migration::{Migrator, MigratorTrait};
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseConnection, DbBackend, Statement,
};
use url::Url;
use uuid::Uuid;

const ADMIN_URL: &str = "PIONEER_TEST_POSTGRES_ADMIN_URL";
pub const RUN_PREFIX: &str = "PIONEER_TEST_POSTGRES_RUN_PREFIX";

struct Admin {
    url: Url,
    connection: DatabaseConnection,
}

impl Admin {
    async fn connect() -> Result<Self> {
        let url = env::var(ADMIN_URL).with_context(|| format!("{ADMIN_URL} must be set"))?;
        let url = Url::parse(&url).map_err(|_| anyhow!("{ADMIN_URL} must be a PostgreSQL URL"))?;
        ensure!(
            matches!(url.scheme(), "postgres" | "postgresql")
                && !url.username().is_empty()
                && url.path() == "/postgres",
            "{ADMIN_URL} must name an explicit role and the postgres maintenance database"
        );
        let connection = connect(url.clone()).await?;
        Ok(Self { url, connection })
    }

    fn database_url(&self, name: &str) -> Url {
        let mut url = self.url.clone();
        url.set_path(name);
        url
    }

    async fn create(&self, name: &str, template: &str) -> Result<()> {
        // CREATE DATABASE does not support parameters for identifiers.
        // Both names are generated in this module or validated by run_prefix.
        self.connection
            .execute_unprepared(&format!(
                r#"CREATE DATABASE "{name}" TEMPLATE "{template}""#
            ))
            .await
            .context("create isolated PostgreSQL database")?;
        Ok(())
    }

    async fn drop_database(&self, name: &str) -> Result<()> {
        ensure!(
            name.starts_with("pioneer_test_")
                && name
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'),
            "invalid isolated database name",
        );
        self.connection
            .execute_unprepared(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#))
            .await
            .context("drop isolated PostgreSQL database")?;
        Ok(())
    }
}

fn connect_options(url: &Url) -> ConnectOptions {
    let mut options = ConnectOptions::new(url.to_string());
    options
        .min_connections(0)
        .max_connections(4)
        .sqlx_logging(false);
    options
}

async fn connect(url: Url) -> Result<DatabaseConnection> {
    connect_with_options(connect_options(&url)).await
}

async fn connect_with_options(options: ConnectOptions) -> Result<DatabaseConnection> {
    Database::connect(options)
        .await
        .map_err(|_| anyhow!("connect to PostgreSQL test server"))
}

pub struct TestDatabase {
    url: Url,
    connection: DatabaseConnection,
    additional_pools: Mutex<Vec<DatabaseConnection>>,
}

impl TestDatabase {
    /// Open another pool on the same isolated database for concurrent scenarios.
    pub async fn connect(&self) -> Result<DatabaseConnection> {
        self.open_pool(connect_options(&self.url)).await
    }

    /// Use a smaller pool when testing waiters at the admission limit.
    pub async fn connect_with_limit(&self, max_connections: u32) -> Result<DatabaseConnection> {
        self.connect_with_options(|options| {
            options.max_connections(max_connections);
        })
        .await
    }

    /// Open a reader pool whose PostgreSQL sessions start read-only.
    pub async fn connect_read_only(&self) -> Result<DatabaseConnection> {
        self.connect_with_options(|options| {
            options.map_sqlx_postgres_opts(|options| {
                options.options([("default_transaction_read_only", "on")])
            });
        })
        .await
    }

    /// Open a pool on this isolated database with test-specific session or pool
    /// settings. The fixture closes it alongside its other pools.
    pub async fn connect_with_options(
        &self,
        configure: impl FnOnce(&mut ConnectOptions),
    ) -> Result<DatabaseConnection> {
        let mut options = connect_options(&self.url);
        configure(&mut options);
        self.open_pool(options).await
    }

    async fn open_pool(&self, options: ConnectOptions) -> Result<DatabaseConnection> {
        let pool = connect_with_options(options).await?;
        self.additional_pools.lock().unwrap().push(pool.clone());
        Ok(pool)
    }

    pub fn connection(&self) -> &DatabaseConnection {
        &self.connection
    }

    pub fn url(&self) -> &Url {
        &self.url
    }

    async fn close(self) -> Result<()> {
        let mut pools = self.additional_pools.into_inner().unwrap();
        pools.push(self.connection);
        tokio::time::timeout(
            Duration::from_secs(5),
            futures_util::future::try_join_all(pools.into_iter().map(DatabaseConnection::close)),
        )
        .await
        .context("timed out closing isolated PostgreSQL test pools")??;
        Ok(())
    }
}

fn new_prefix() -> String {
    format!("pioneer_test_{}_", Uuid::new_v4().simple())
}

fn run_prefix() -> Result<Option<String>> {
    let prefix = match env::var(RUN_PREFIX) {
        Ok(prefix) => prefix,
        Err(env::VarError::NotPresent) => return Ok(None),
        Err(_) => return Err(anyhow!("invalid {RUN_PREFIX}")),
    };
    let id = prefix
        .strip_prefix("pioneer_test_")
        .and_then(|prefix| prefix.strip_suffix('_'));
    ensure!(
        id.is_some_and(|id| id.len() == 32
            && id
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())),
        "invalid {RUN_PREFIX}",
    );
    Ok(Some(prefix))
}

/// Run a test against a private clone of the current test run's migrated schema.
pub async fn with_database<T>(test: impl AsyncFnOnce(&TestDatabase) -> Result<T>) -> Result<T> {
    let prefix = run_prefix()?.context("run database tests through pioneer-postgres-test")?;
    with_isolated_database(&prefix, &format!("{prefix}template"), test).await
}

/// Run a migration test against an empty database.
pub async fn with_empty_database<T>(
    test: impl AsyncFnOnce(&TestDatabase) -> Result<T>,
) -> Result<T> {
    let prefix = run_prefix()?.unwrap_or_else(new_prefix);
    with_isolated_database(&prefix, "template0", test).await
}

async fn with_isolated_database<T>(
    prefix: &str,
    template: &str,
    test: impl AsyncFnOnce(&TestDatabase) -> Result<T>,
) -> Result<T> {
    let admin = Admin::connect().await?;
    // Keep the entire name under the 63-byte PostgreSQL identifier limit.
    let suffix = Uuid::new_v4().simple().to_string();
    let name = format!("{prefix}{}", &suffix[..16]);
    admin.create(&name, template).await?;
    let outcome = AssertUnwindSafe(async {
        let url = admin.database_url(&name);
        let connection = connect(url.clone()).await?;
        let database = TestDatabase {
            url,
            connection,
            additional_pools: Mutex::new(Vec::new()),
        };
        let result = AssertUnwindSafe(test(&database)).catch_unwind().await;
        let closed = database.close().await;
        match result {
            Ok(result) => result.and_then(|value| {
                closed.context("close PostgreSQL test pool")?;
                Ok(value)
            }),
            Err(panic) => resume_unwind(panic),
        }
    })
    .catch_unwind()
    .await;
    let cleanup = admin.drop_database(&name).await;
    finish(outcome, cleanup)
}

fn finish<T>(outcome: std::thread::Result<Result<T>>, cleanup: Result<()>) -> Result<T> {
    match outcome {
        Ok(Ok(result)) => cleanup.map(|()| result),
        Ok(Err(error)) => match cleanup {
            Ok(()) => Err(error),
            Err(cleanup) => {
                Err(error.context(format!("PostgreSQL test cleanup also failed: {cleanup:#}")))
            }
        },
        Err(panic) => {
            if let Err(error) = cleanup {
                eprintln!("PostgreSQL test cleanup failed: {error:#}");
            }
            resume_unwind(panic);
        }
    }
}

/// Prepare one migrated template, run the test runner, and remove its databases.
pub async fn with_test_run<T, F: Future<Output = Result<T>>>(
    run: impl FnOnce(String) -> F,
) -> Result<T> {
    let admin = Admin::connect().await?;
    let prefix = new_prefix();
    let template = format!("{prefix}template");
    admin.create(&template, "template0").await?;
    let outcome = AssertUnwindSafe(async {
        let connection = connect(admin.database_url(&template)).await?;
        let migrated = Migrator::up(&connection, None).await;
        let closed = connection.close().await;
        migrated.context("migrate PostgreSQL test template")?;
        closed.context("close PostgreSQL test template pool")?;
        // ALTER DATABASE has no SeaQuery builder. Prevent any further pool from
        // connecting so every test can clone the same schema concurrently.
        admin
            .connection
            .execute_unprepared(&format!(
                r#"ALTER DATABASE "{template}" WITH ALLOW_CONNECTIONS false"#
            ))
            .await
            .context("seal PostgreSQL test template")?;
        run(prefix.clone()).await
    })
    .catch_unwind()
    .await;
    let cleanup = clean_test_run(&admin, &prefix).await;
    finish(outcome, cleanup)
}

async fn clean_test_run(admin: &Admin, prefix: &str) -> Result<()> {
    let databases = admin
        .connection
        .query_all_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "SELECT datname::text AS name FROM pg_database WHERE starts_with(datname::text, $1)",
            [prefix.into()],
        ))
        .await
        .context("list databases from this PostgreSQL test run")?;
    let mut first_error = None;
    for database in databases {
        let name: String = database.try_get("", "name")?;
        ensure!(name.starts_with(prefix), "database is outside the test run");
        if let Err(error) = admin.drop_database(&name).await {
            first_error.get_or_insert(error);
        }
    }
    first_error.map_or(Ok(()), Err)
}
