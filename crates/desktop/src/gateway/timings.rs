use anyhow::{Result, bail};
use pioneer_config::GatewayRuntimeConfig;
use std::time::Duration;

#[derive(Clone, Copy, Debug)]
pub(crate) struct GatewayTimings {
    pub(crate) connect_timeout: Duration,
    pub(crate) startup_timeout: Duration,
    pub(crate) poll_interval: Duration,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct GatewayWsTimings {
    pub(crate) connect_timeout: Duration,
    pub(crate) ping_interval: Duration,
    pub(crate) pong_timeout: Duration,
    pub(crate) reconnect_initial: Duration,
    pub(crate) reconnect_max: Duration,
    pub(crate) reconnect_jitter_percent: u8,
}

impl GatewayTimings {
    pub(crate) fn from_config(config: &GatewayRuntimeConfig) -> Result<Self> {
        if config.connect_timeout_ms == 0 {
            bail!("{}", t!("errors.config.connect_timeout_positive"));
        }
        if config.startup_timeout_ms == 0 {
            bail!("{}", t!("errors.config.startup_timeout_positive"));
        }
        if config.poll_interval_ms == 0 {
            bail!("{}", t!("errors.config.poll_interval_positive"));
        }

        Ok(Self {
            connect_timeout: Duration::from_millis(config.connect_timeout_ms),
            startup_timeout: Duration::from_millis(config.startup_timeout_ms),
            poll_interval: Duration::from_millis(config.poll_interval_ms),
        })
    }
}

impl GatewayWsTimings {
    pub(crate) fn from_config(config: &GatewayRuntimeConfig) -> Result<Self> {
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

        Ok(Self {
            connect_timeout: Duration::from_millis(config.connect_timeout_ms),
            ping_interval: Duration::from_millis(config.ws_ping_interval_ms),
            pong_timeout: Duration::from_millis(config.ws_pong_timeout_ms),
            reconnect_initial: Duration::from_millis(config.ws_reconnect_initial_ms),
            reconnect_max: Duration::from_millis(config.ws_reconnect_max_ms),
            reconnect_jitter_percent: config.ws_reconnect_jitter_percent,
        })
    }
}
