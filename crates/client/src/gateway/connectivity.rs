//! Shell-neutral Gateway reachability helpers for canonical base URLs.

use std::{
    error::Error,
    fmt,
    net::{SocketAddr, TcpStream, ToSocketAddrs},
    time::Duration,
};

use super::endpoint::GatewayBaseUrl;
pub use pioneer_protocol::DEFAULT_GATEWAY_PORT;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GatewayAddressError {
    ResolutionFailed,
    NoResolvedAddress,
}

pub fn resolve_gateway_socket_addrs(
    gateway_base_url: &GatewayBaseUrl,
) -> Result<Vec<SocketAddr>, GatewayAddressError> {
    let addrs: Vec<SocketAddr> = gateway_base_url
        .socket_address_input()
        .to_socket_addrs()
        .map_err(|_| GatewayAddressError::ResolutionFailed)?
        .collect();
    if addrs.is_empty() {
        return Err(GatewayAddressError::NoResolvedAddress);
    }
    Ok(addrs)
}

pub fn is_gateway_reachable(
    gateway_base_url: &GatewayBaseUrl,
    connect_timeout: Duration,
) -> Result<bool, GatewayAddressError> {
    let addrs = resolve_gateway_socket_addrs(gateway_base_url)?;
    Ok(addrs
        .iter()
        .any(|address| TcpStream::connect_timeout(address, connect_timeout).is_ok()))
}

impl fmt::Display for GatewayAddressError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ResolutionFailed => "Gateway host resolution failed",
            Self::NoResolvedAddress => "Gateway host did not resolve to any address",
        })
    }
}

impl Error for GatewayAddressError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_base_provides_deterministic_socket_input() {
        let base = GatewayBaseUrl::parse_presentation("https://127.0.0.1/pioneer").unwrap();
        assert_eq!(base.socket_address_input(), "127.0.0.1:443");
        assert_eq!(resolve_gateway_socket_addrs(&base).unwrap()[0].port(), 443);
    }
}
