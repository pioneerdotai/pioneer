use anyhow::Context as _;

fn main() -> anyhow::Result<()> {
    let sentry_guard =
        pioneer_observability::init_sentry(pioneer_observability::SentryTarget::Shared);
    pioneer_observability::init_tracing(sentry_guard.is_some());

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to build gateway Tokio runtime")?;

    let result = runtime.block_on(pioneer_gateway::run_gateway_until_shutdown());
    if let Err(error) = &result {
        pioneer_observability::capture_anyhow(error);
    }
    result
}
