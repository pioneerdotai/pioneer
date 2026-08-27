use anyhow::{Context, Result, bail};
use pioneer_client::gateway::{
    connectivity::is_gateway_reachable as is_client_gateway_reachable,
    endpoint::GatewayBaseUrl,
    setup::{
        RemoteGatewayValidation, RemoteGatewayValidationError,
        validate_remote_gateway_base_url as validate_client_remote_gateway_base_url,
    },
};
use pioneer_protocol::{GatewayReadinessSnapshot, GatewayReadinessStatus};
use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LocalGatewayReadiness {
    Unavailable,
    Gateway(GatewayReadinessStatus),
    IncompatibleService,
}

impl LocalGatewayReadiness {
    pub(crate) const fn status(self) -> Option<GatewayReadinessStatus> {
        match self {
            Self::Gateway(status) => Some(status),
            Self::Unavailable | Self::IncompatibleService => None,
        }
    }
}

pub(crate) fn is_gateway_reachable(
    gateway_base_url: &GatewayBaseUrl,
    connect_timeout: Duration,
) -> Result<bool> {
    is_client_gateway_reachable(gateway_base_url, connect_timeout).context(
        t!(
            "errors.gateway.resolve_failed",
            listen_addr = gateway_base_url.as_str()
        )
        .to_string(),
    )
}

pub(crate) fn is_local_gateway_reachable(
    listen_addr: &str,
    connect_timeout: Duration,
) -> Result<bool> {
    let base = GatewayBaseUrl::from_local_listen_addr(listen_addr)
        .context("failed to derive local Gateway destination")?;
    is_gateway_reachable(&base, connect_timeout)
}

pub(crate) fn local_gateway_readiness(
    listen_addr: &str,
    request_timeout: Duration,
) -> Result<LocalGatewayReadiness> {
    let base = GatewayBaseUrl::from_local_listen_addr(listen_addr)
        .context("failed to derive local Gateway readiness destination")?;
    let client = reqwest::blocking::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(request_timeout)
        .build()
        .context("failed to build local Gateway readiness client")?;
    let response = match client.get(base.readiness_url()).send() {
        Ok(response) => response,
        Err(error) if error.is_connect() || error.is_timeout() => {
            return Ok(LocalGatewayReadiness::Unavailable);
        }
        Err(error) => return Err(error).context("local Gateway readiness request failed"),
    };
    let gateway_json_response = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("application/json"));
    let snapshot = match response.json::<GatewayReadinessSnapshot>() {
        Ok(snapshot) => snapshot,
        Err(error) if error.is_connect() || error.is_timeout() || gateway_json_response => {
            return Ok(LocalGatewayReadiness::Unavailable);
        }
        Err(_) => {
            // A listening process that does not speak the Gateway readiness
            // protocol is not a ready Gateway. Callers combine this with the TCP
            // probe to report a local-address conflict without trusting the port.
            return Ok(LocalGatewayReadiness::IncompatibleService);
        }
    };
    Ok(LocalGatewayReadiness::Gateway(snapshot.status))
}

pub(crate) fn is_local_gateway_accepting_sessions(
    listen_addr: &str,
    request_timeout: Duration,
) -> Result<bool> {
    Ok(local_gateway_readiness(listen_addr, request_timeout)?
        .status()
        .is_some_and(GatewayReadinessStatus::accepts_sessions))
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
            t!(
                "errors.gateway.validation_timeout_positive",
                timeout_ms = timeout_ms
            )
        ),
        Err(RemoteGatewayValidationError::InvalidGatewayBaseUrl(error)) => Err(error)
            .context(t!("errors.gateway.invalid_address", normalized = "[redacted]").to_string()),
        Err(RemoteGatewayValidationError::ResolveFailed { source, .. }) => Err(source)
            .context(t!("errors.gateway.resolve_failed", listen_addr = "[redacted]").to_string()),
    }
}
