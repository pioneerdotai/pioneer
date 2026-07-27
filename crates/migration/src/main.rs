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
            cli::run_cli(migration::Migrator).await;
            Ok(())
        })
}
