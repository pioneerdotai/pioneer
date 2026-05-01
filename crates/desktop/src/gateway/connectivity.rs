use anyhow::{Context, Result, bail};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpStream, ToSocketAddrs};
use std::time::Duration;

pub(crate) fn normalize_address(address: &str) -> Result<String> {
    let normalized = address.trim();

    if normalized.is_empty() {
        bail!("{}", t!("errors.gateway.address_empty"));
    }

    resolve_addrs(normalized).with_context(|| {
        t!("errors.gateway.invalid_address", normalized = normalized).to_string()
    })?;

    Ok(normalized.to_owned())
}

pub(crate) fn is_gateway_reachable(listen_addr: &str, connect_timeout: Duration) -> Result<bool> {
    let addrs = resolve_addrs(listen_addr)?;

    for addr in addrs {
        let addr = normalize_unspecified_addr(addr);
        if TcpStream::connect_timeout(&addr, connect_timeout).is_ok() {
            return Ok(true);
        }
    }

    Ok(false)
}

fn resolve_addrs(listen_addr: &str) -> Result<Vec<SocketAddr>> {
    let addrs: Vec<SocketAddr> = listen_addr
        .to_socket_addrs()
        .with_context(|| {
            t!("errors.gateway.resolve_failed", listen_addr = listen_addr).to_string()
        })?
        .collect();

    if addrs.is_empty() {
        bail!(
            "{}",
            t!("errors.gateway.resolve_empty", listen_addr = listen_addr)
        );
    }

    Ok(addrs)
}

fn normalize_unspecified_addr(addr: SocketAddr) -> SocketAddr {
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
