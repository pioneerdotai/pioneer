use anyhow::{Result, bail};
pub(crate) use pioneer_client::gateway::timings::{GatewayTimings, GatewayWsTimings};
use pioneer_config::GatewayRuntimeConfig;

pub(crate) fn gateway_timings_from_config(config: &GatewayRuntimeConfig) -> Result<GatewayTimings> {
    if config.connect_timeout_ms == 0 {
        bail!("{}", t!("errors.config.connect_timeout_positive"));
    }
    if config.startup_timeout_ms == 0 {
        bail!("{}", t!("errors.config.startup_timeout_positive"));
    }
    if config.poll_interval_ms == 0 {
        bail!("{}", t!("errors.config.poll_interval_positive"));
    }

    Ok(GatewayTimings::from_millis(
        config.connect_timeout_ms,
        config.startup_timeout_ms,
        config.poll_interval_ms,
    )?)
}

pub(crate) fn gateway_ws_timings_from_config(
    config: &GatewayRuntimeConfig,
) -> Result<GatewayWsTimings> {
    if config.connect_timeout_ms == 0 {
        bail!("{}", t!("errors.config.connect_timeout_positive"));
    }
    if config.ws_ping_interval_ms == 0 {
        bail!("ws_ping_interval_ms in config must be positive");
    }
    if config.ws_pong_timeout_ms == 0 {
        bail!("ws_pong_timeout_ms in config must be positive");
    }
    if config.ws_reconnect_initial_ms == 0 {
        bail!("ws_reconnect_initial_ms in config must be positive");
    }
    if config.ws_reconnect_max_ms == 0 {
        bail!("ws_reconnect_max_ms in config must be positive");
    }
    if config.ws_reconnect_initial_ms > config.ws_reconnect_max_ms {
        bail!("ws_reconnect_initial_ms must be <= ws_reconnect_max_ms");
    }
    if config.ws_reconnect_jitter_percent > 100 {
        bail!("ws_reconnect_jitter_percent must be <= 100");
    }

    Ok(GatewayWsTimings::from_millis(
        config.connect_timeout_ms,
        config.ws_ping_interval_ms,
        config.ws_pong_timeout_ms,
        config.ws_reconnect_initial_ms,
        config.ws_reconnect_max_ms,
        config.ws_reconnect_jitter_percent,
    )?)
}
