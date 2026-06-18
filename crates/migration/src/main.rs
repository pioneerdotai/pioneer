use anyhow::Result;
use sea_orm_migration::prelude::*;

fn main() -> Result<()> {
    let sentry_guard =
        pioneer_observability::init_sentry(pioneer_observability::SentryTarget::Shared);
    pioneer_observability::init_tracing(sentry_guard.is_some());

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            cli::run_cli(migration::Migrator).await;
            Ok(())
        })
}
