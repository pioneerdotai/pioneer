use anyhow::{Result, anyhow};
pub(crate) use pioneer_client::gateway::timings::{
    GatewayTimingError, GatewayTimings, GatewayWsTimings,
};
use pioneer_config::GatewayRuntimeConfig;

pub(crate) fn gateway_timings_from_config(config: &GatewayRuntimeConfig) -> Result<GatewayTimings> {
    GatewayTimings::from_millis(
        config.connect_timeout_ms,
        config.startup_timeout_ms,
        config.poll_interval_ms,
    )
    .map_err(|error| anyhow!(gateway_timing_error_text(error)))
}

pub(crate) fn gateway_ws_timings_from_config(
    config: &GatewayRuntimeConfig,
) -> Result<GatewayWsTimings> {
    GatewayWsTimings::from_millis(
        config.connect_timeout_ms,
        config.ws_ping_interval_ms,
        config.ws_pong_timeout_ms,
        config.ws_reconnect_initial_ms,
        config.ws_reconnect_max_ms,
        config.ws_reconnect_jitter_percent,
    )
    .map_err(|error| anyhow!(gateway_timing_error_text(error)))
}

fn gateway_timing_error_text(error: GatewayTimingError) -> String {
    match error {
        GatewayTimingError::FieldMustBePositive(field) => match field {
            "connect_timeout_ms" => t!("errors.config.connect_timeout_positive").to_string(),
            "startup_timeout_ms" => t!("errors.config.startup_timeout_positive").to_string(),
            "poll_interval_ms" => t!("errors.config.poll_interval_positive").to_string(),
            "ws_ping_interval_ms" => t!("errors.config.ws_ping_interval_positive").to_string(),
            "ws_pong_timeout_ms" => t!("errors.config.ws_pong_timeout_positive").to_string(),
            "ws_reconnect_initial_ms" => {
                t!("errors.config.ws_reconnect_initial_positive").to_string()
            }
            "ws_reconnect_max_ms" => t!("errors.config.ws_reconnect_max_positive").to_string(),
            field => t!("errors.config.gateway_field_positive", field = field).to_string(),
        },
        GatewayTimingError::ReconnectInitialGreaterThanMax => {
            t!("errors.config.ws_reconnect_initial_le_max").to_string()
        }
        GatewayTimingError::ReconnectJitterPercentTooHigh { value } => t!(
            "errors.config.ws_reconnect_jitter_percent_max",
            value = value
        )
        .to_string(),
    }
}
