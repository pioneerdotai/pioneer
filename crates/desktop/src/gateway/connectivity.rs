use anyhow::{Context, Result, bail};
use pioneer_client::gateway::{
    connectivity::is_gateway_reachable as is_client_gateway_reachable,
    endpoint::GatewayBaseUrl,
    setup::{
        RemoteGatewayValidation, RemoteGatewayValidationError,
        validate_remote_gateway_base_url as validate_client_remote_gateway_base_url,
    },
};
use std::time::Duration;

pub(crate) fn is_gateway_reachable(
    gateway_base_url: &GatewayBaseUrl,
    connect_timeout: Duration,
) -> Result<bool> {
    is_client_gateway_reachable(gateway_base_url, connect_timeout)
        .context(t!("errors.gateway.resolve_failed", listen_addr = gateway_base_url.as_str()).to_string())
}

pub(crate) fn is_local_gateway_reachable(
    listen_addr: &str,
    connect_timeout: Duration,
) -> Result<bool> {
    let base = GatewayBaseUrl::from_local_listen_addr(listen_addr)
        .context("failed to derive local Gateway destination")?;
    is_gateway_reachable(&base, connect_timeout)
}

pub(crate) fn validate_remote_gateway_base_url(
    gateway_base_url: &str,
    connect_timeout: Duration,
) -> Result<GatewayBaseUrl> {
    match validate_client_remote_gateway_base_url(gateway_base_url, connect_timeout) {
        Ok(RemoteGatewayValidation::Reachable {
            gateway_base_url, ..
        }) => Ok(gateway_base_url),
        Ok(RemoteGatewayValidation::Unreachable {
            gateway_base_url, ..
        }) => bail!(
            "{}",
            t!(
                "errors.gateway.unreachable_verify",
                gateway_base_url = gateway_base_url.as_str()
            )
        ),
        Err(RemoteGatewayValidationError::InvalidTimeout { timeout_ms }) => bail!(
            "{}",
            t!("errors.gateway.validation_timeout_positive", timeout_ms = timeout_ms)
        ),
        Err(RemoteGatewayValidationError::InvalidGatewayBaseUrl(error)) => {
            Err(error).context(t!("errors.gateway.invalid_address", normalized = "[redacted]").to_string())
        }
        Err(RemoteGatewayValidationError::ResolveFailed { source, .. }) => {
            Err(source).context(t!("errors.gateway.resolve_failed", listen_addr = "[redacted]").to_string())
        }
    }
}
