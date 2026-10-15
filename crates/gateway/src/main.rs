use anyhow::Context as _;

fn main() -> anyhow::Result<()> {
    let startup = pioneer_observability::GatewayStartupTrace::start();
    // The persisted gateway preference is loaded inside the runtime. Keep all
    // external telemetry closed until that happens so a previous opt-out is
    // honored from the first startup event.
    pioneer_observability::set_telemetry_enabled(false);
    let sentry_guard =
        pioneer_observability::init_sentry(pioneer_observability::SentryTarget::Shared);
    pioneer_observability::init_tracing(sentry_guard.is_some());

    let runtime_stage = startup.stage(pioneer_observability::GatewayStartupStage::RuntimeBuild);
    let runtime = pioneer_gateway::build_gateway_runtime()
        .context("failed to build gateway Tokio runtime")?;
    runtime_stage.succeed();

    let result = runtime.block_on(pioneer_gateway::run_gateway_until_shutdown_with_startup(
        startup,
    ));
    if let Err(error) = &result {
        pioneer_observability::capture_anyhow(error);
    }
    result
}
