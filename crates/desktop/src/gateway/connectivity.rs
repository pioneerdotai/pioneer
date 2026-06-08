use anyhow::{Context, Result, bail};
use pioneer_client::gateway::connectivity::{
    GatewayAddressError, is_gateway_reachable as is_client_gateway_reachable,
    normalize_address as normalize_client_address,
};
use pioneer_client::gateway::setup::{
    RemoteGatewayValidation, RemoteGatewayValidationError,
    validate_remote_gateway_address as validate_client_remote_gateway_address,
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

pub(crate) fn validate_remote_gateway_address(
    address: &str,
    connect_timeout: Duration,
) -> Result<String> {
    let trimmed = address.trim();
    match validate_client_remote_gateway_address(address, connect_timeout) {
        Ok(RemoteGatewayValidation::Reachable { address }) => Ok(address),
        Ok(RemoteGatewayValidation::Unreachable { address }) => {
            bail!(
                "{}",
                t!(
                    "errors.gateway.unreachable_verify",
                    address = address.as_str()
                )
            );
        }
        Err(RemoteGatewayValidationError::InvalidAddress(GatewayAddressError::Empty)) => {
            bail!("{}", t!("errors.gateway.address_empty"));
        }
        Err(RemoteGatewayValidationError::InvalidTimeout { timeout_ms }) => {
            bail!(
                "{}",
                t!(
                    "errors.gateway.validation_timeout_positive",
                    timeout_ms = timeout_ms
                )
            );
        }
        Err(RemoteGatewayValidationError::InvalidAddress(error)) => Err(error).with_context(|| {
            t!("errors.gateway.invalid_address", normalized = trimmed).to_string()
        }),
        Err(RemoteGatewayValidationError::ResolveFailed { address, source }) => Err(source)
            .with_context(|| {
                t!(
                    "errors.gateway.resolve_failed",
                    listen_addr = address.as_str()
                )
                .to_string()
            }),
        Err(error @ RemoteGatewayValidationError::InvalidTimings(_))
        | Err(error @ RemoteGatewayValidationError::ConnectionFailed { .. }) => Err(error.into()),
    }
}
