use anyhow::{Context, Result, bail};
use pioneer_client::gateway::connectivity::{
    GatewayAddressError, is_gateway_reachable as is_client_gateway_reachable,
    normalize_address as normalize_client_address,
};
use std::time::Duration;

pub(crate) fn normalize_address(address: &str) -> Result<String> {
    let trimmed = address.trim();
    match normalize_client_address(address) {
        Ok(address) => Ok(address),
        Err(GatewayAddressError::Empty) => {
            bail!("{}", t!("errors.gateway.address_empty"));
        }
        Err(error) => Err(error).with_context(|| {
            t!("errors.gateway.invalid_address", normalized = trimmed).to_string()
        }),
    }
}

pub(crate) fn is_gateway_reachable(listen_addr: &str, connect_timeout: Duration) -> Result<bool> {
    is_client_gateway_reachable(listen_addr, connect_timeout)
        .with_context(|| t!("errors.gateway.resolve_failed", listen_addr = listen_addr).to_string())
}
