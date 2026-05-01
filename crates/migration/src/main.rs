use anyhow::Result;
use sea_orm_migration::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    cli::run_cli(migration::Migrator).await;
    Ok(())
}
