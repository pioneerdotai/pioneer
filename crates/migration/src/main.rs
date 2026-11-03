use anyhow::Result;
use sea_orm_migration::prelude::*;

fn main() -> Result<()> {
    // `sea_orm_migration::cli` installs its own global tracing subscriber.
    // Initializing Pioneer tracing here first makes every migration command
    // panic with `SetGlobalDefaultError` before it can connect to the database.
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            cli::run_cli_with_connection(migration::Migrator, |mut options| async move {
                use sea_orm_migration::sea_orm::{Database, DbErr};
                use std::str::FromStr;
                let parsed =
                    sea_orm_migration::sea_orm::sqlx::sqlite::SqliteConnectOptions::from_str(
                        options.get_url(),
                    )
                    .map_err(|error| {
                        DbErr::Custom(format!(
                            "offline migration requires a file SQLite URL: {error}"
                        ))
                    })?;
                use sea_orm_migration::sea_orm::sqlx::ConnectOptions as _;
                if parsed.to_url_lossy().query_pairs().any(|(key, value)| (key == "mode" && value == "memory") || key == "vfs") {
                    return Err(DbErr::Custom("offline migration ownership requires an ordinary file database, without a custom VFS".into()));
                }
                let ownership = pioneer_sqlite::ownership::Opening::acquire(parsed.get_filename())
                    .map_err(|error| DbErr::Custom(format!("{error:#}")))?;
                // SQLx's URL parser accepts only connection flags (mode/cache/
                // immutable/vfs), no durable PRAGMAs. Preserve CLI options and
                // canonicalize only the filename in the ownership callback.
                options.max_connections(1);
                ownership.configure(&mut options);
                let database = Database::connect(options).await?;
                ownership.connected();
                Ok(database)
            })
            .await;
            Ok(())
        })
}
