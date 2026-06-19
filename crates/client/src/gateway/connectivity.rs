//! Shell-neutral gateway address normalization helpers.

use std::{
    error::Error,
    fmt,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpStream, ToSocketAddrs},
    time::Duration,
};
use url::Url;

pub const DEFAULT_GATEWAY_PORT: u16 = 17878;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GatewayAddressError {
    Empty,
    InvalidAddress { input: String, reason: String },
}

impl GatewayAddressError {
    fn invalid(input: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::InvalidAddress {
            input: input.into(),
            reason: reason.into(),
        }
    }
}

pub fn normalize_address(address: &str) -> Result<String, GatewayAddressError> {
    let trimmed = address.trim();

    if trimmed.is_empty() {
        return Err(GatewayAddressError::Empty);
    }

    if trimmed.starts_with("ws://") || trimmed.starts_with("wss://") {
        return normalize_ws_address(trimmed);
    }

    if trimmed.starts_with("https://") {
        return normalize_https_address(trimmed);
    }

    if trimmed.contains("://") {
        return Err(GatewayAddressError::invalid(
            trimmed,
            "gateway address must be an HTTPS URL, websocket URL, host, or host:port",
        ));
    }

    normalize_host_port_address(trimmed)
}

pub fn resolve_socket_address_input(address: &str) -> Result<String, GatewayAddressError> {
    let trimmed = address.trim();

    if trimmed.is_empty() {
        return Err(GatewayAddressError::Empty);
    }

    if trimmed.starts_with("ws://") || trimmed.starts_with("wss://") {
        let url = Url::parse(trimmed).map_err(|error| {
            GatewayAddressError::invalid(trimmed, format!("invalid websocket URL: {error}"))
        })?;
        let host = url
            .host_str()
            .filter(|host| !host.trim().is_empty())
            .ok_or_else(|| GatewayAddressError::invalid(trimmed, "address must include a host"))?;
        let port = url
            .port()
            .or_else(|| default_port_for_ws_scheme(url.scheme()));

        let Some(port) = port else {
            return Err(GatewayAddressError::invalid(
                trimmed,
                "websocket gateway address must include a port",
            ));
        };

        return Ok(format_host_port(host, port));
    }

    if trimmed.starts_with("https://") {
        let url = Url::parse(trimmed).map_err(|error| {
            GatewayAddressError::invalid(trimmed, format!("invalid HTTPS URL: {error}"))
        })?;
        let host = url
            .host_str()
            .filter(|host| !host.trim().is_empty())
            .ok_or_else(|| GatewayAddressError::invalid(trimmed, "address must include a host"))?;
        let port = url.port().unwrap_or(443);
        return Ok(format_host_port(host, port));
    }

    normalize_host_port_address(trimmed)
}

pub fn normalize_unspecified_addr(addr: SocketAddr) -> SocketAddr {
    match addr {
        SocketAddr::V4(v4) if v4.ip().is_unspecified() => {
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), v4.port())
        }
        SocketAddr::V6(v6) if v6.ip().is_unspecified() => {
            SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), v6.port())
        }
        _ => addr,
    }
}

pub fn resolve_gateway_socket_addrs(
    listen_addr: &str,
) -> Result<Vec<SocketAddr>, GatewayAddressError> {
    let socket_addr = resolve_socket_address_input(listen_addr)?;
    let addrs: Vec<SocketAddr> = socket_addr
        .to_socket_addrs()
        .map_err(|error| {
            GatewayAddressError::invalid(
                listen_addr,
                format!("failed to resolve gateway address: {error}"),
            )
        })?
        .map(normalize_unspecified_addr)
        .collect();

    if addrs.is_empty() {
        return Err(GatewayAddressError::invalid(
            listen_addr,
            "gateway address did not resolve to any socket address",
        ));
    }

    Ok(addrs)
}

pub fn is_gateway_reachable(
    listen_addr: &str,
    connect_timeout: Duration,
) -> Result<bool, GatewayAddressError> {
    let addrs = resolve_gateway_socket_addrs(listen_addr)?;

    for addr in addrs {
        if TcpStream::connect_timeout(&addr, connect_timeout).is_ok() {
            return Ok(true);
        }
    }

    Ok(false)
}

fn normalize_ws_address(address: &str) -> Result<String, GatewayAddressError> {
    let url = Url::parse(address).map_err(|error| {
        GatewayAddressError::invalid(address, format!("invalid websocket URL: {error}"))
    })?;
    match url.scheme() {
        "ws" | "wss" => {}
        _ => {
            return Err(GatewayAddressError::invalid(
                address,
                "unsupported websocket gateway scheme",
            ));
        }
    }

    if url.host_str().is_none() {
        return Err(GatewayAddressError::invalid(
            address,
            "websocket gateway address must include a host",
        ));
    }

    Ok(address.to_owned())
}

fn normalize_https_address(address: &str) -> Result<String, GatewayAddressError> {
    let url = Url::parse(address).map_err(|error| {
        GatewayAddressError::invalid(address, format!("invalid HTTPS URL: {error}"))
    })?;
    if url.scheme() != "https" {
        return Err(GatewayAddressError::invalid(
            address,
            "unsupported gateway URL scheme",
        ));
    }
    if url.host_str().is_none() {
        return Err(GatewayAddressError::invalid(
            address,
            "gateway URL must include a host",
        ));
    }

    Ok(address.to_owned())
}

fn normalize_host_port_address(address: &str) -> Result<String, GatewayAddressError> {
    if let Ok(ip) = address.parse::<IpAddr>() {
        let host = ip.to_string();
        return Ok(format_host_port(host.as_str(), DEFAULT_GATEWAY_PORT));
    }

    if let Some(ip) = address
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .and_then(|value| value.parse::<IpAddr>().ok())
    {
        let host = ip.to_string();
        return Ok(format_host_port(host.as_str(), DEFAULT_GATEWAY_PORT));
    }

    let url = Url::parse(format!("ws://{address}").as_str()).map_err(|error| {
        GatewayAddressError::invalid(address, format!("invalid host or host:port: {error}"))
    })?;

    if !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(GatewayAddressError::invalid(
            address,
            "gateway address must be a host or host:port",
        ));
    }

    let host = url
        .host_str()
        .filter(|host| !host.trim().is_empty())
        .ok_or_else(|| GatewayAddressError::invalid(address, "address must include a host"))?;
    let port = url.port().unwrap_or(DEFAULT_GATEWAY_PORT);

    Ok(format_host_port(host, port))
}

fn default_port_for_ws_scheme(scheme: &str) -> Option<u16> {
    match scheme {
        "ws" => Some(80),
        "wss" => Some(443),
        _ => None,
    }
}

fn format_host_port(host: &str, port: u16) -> String {
    if host.starts_with('[') && host.ends_with(']') {
        format!("{host}:{port}")
    } else if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

impl fmt::Display for GatewayAddressError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => write!(f, "gateway address must not be empty"),
            Self::InvalidAddress { input, reason } => {
                write!(f, "invalid gateway address `{input}`: {reason}")
            }
        }
    }
}

impl Error for GatewayAddressError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_gateway_socket_addrs_normalizes_unspecified_addresses() {
        let addrs = resolve_gateway_socket_addrs("0.0.0.0:17878").unwrap();

        assert!(
            addrs
                .iter()
                .any(|addr| addr.to_string() == "127.0.0.1:17878")
        );
    }

    #[test]
    fn is_gateway_reachable_detects_open_socket() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();

        assert!(is_gateway_reachable(&address, Duration::from_millis(100)).unwrap());
    }
}
