use anyhow::Result;
use sea_orm_migration::prelude::*;

fn main() -> Result<()> {
    // Retain ownership until after the utility's runtime (and its SQLx
    // workers) has shut down, including error/panic unwinding.
    let ownership = std::sync::Arc::new(std::sync::Mutex::new(None));
    let owned = ownership.clone();
    // `sea_orm_migration::cli` installs its own global tracing subscriber.
    // Initializing Pioneer tracing here first makes every migration command
    // panic with `SetGlobalDefaultError` before it can connect to the database.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        cli::run_cli_with_connection(migration::Migrator, move |mut options| async move {
            use sea_orm_migration::sea_orm::{Database, DbErr, sqlx::sqlite::SqliteConnectOptions};
            use std::str::FromStr;
            let sqlite = SqliteConnectOptions::from_str(options.get_url()).map_err(|_| {
                DbErr::Custom("migration utility requires a SQLite file URL".into())
            })?;
            if !sqlite.get_filename().as_os_str().is_empty()
                && sqlite.get_filename() != std::path::Path::new(":memory:")
            {
                let lease = pioneer_sqlite::WriterOwnership::acquire(sqlite.get_filename())
                    .map_err(|error| DbErr::Custom(error.to_string()))?;
                *owned.lock().unwrap() = Some(lease.clone());
                options.map_sqlx_sqlite_pool_opts(move |options| {
                    let lease = lease.clone();
                    options.after_connect(move |connection, _| {
                        let lease = lease.clone();
                        Box::pin(async move { lease.retain_on_connection(connection).await })
                    })
                });
            }
            Database::connect(options).await
        })
        .await;
        Ok(())
    })
}
