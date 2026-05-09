use anyhow::{Result, bail};

use super::{GatewayServiceWarning, ServiceSettings};

pub fn start_gateway_service(_settings: &ServiceSettings) -> Result<Vec<GatewayServiceWarning>> {
    bail!("`pioneer start` is not supported on this operating system");
}

pub fn stop_gateway_service(_settings: &ServiceSettings) -> Result<()> {
    bail!("`pioneer stop` is not supported on this operating system");
}

pub fn is_gateway_service_active(_settings: &ServiceSettings) -> Result<bool> {
    Ok(false)
}
