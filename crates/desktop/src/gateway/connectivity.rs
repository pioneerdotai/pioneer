use anyhow::{Context, Result, bail};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpStream, ToSocketAddrs};
use std::time::Duration;
use url::Url;

const DEFAULT_GATEWAY_PORT: u16 = 17878;

pub(crate) fn normalize_address(address: &str) -> Result<String> {
    let trimmed = address.trim();

    if trimmed.is_empty() {
        bail!("{}", t!("errors.gateway.address_empty"));
    }

    if trimmed.starts_with("ws://") || trimmed.starts_with("wss://") {
        return normalize_ws_address(trimmed).with_context(|| {
            t!("errors.gateway.invalid_address", normalized = trimmed).to_string()
        });
    }

    if trimmed.contains("://") {
        bail!(
            "{}",
            t!("errors.gateway.invalid_address", normalized = trimmed)
        );
    }

    normalize_host_port_address(trimmed)
        .with_context(|| t!("errors.gateway.invalid_address", normalized = trimmed).to_string())
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

fn normalize_ws_address(address: &str) -> Result<String> {
    let url = Url::parse(address)?;
    match url.scheme() {
        "ws" | "wss" => {}
        _ => bail!("unsupported websocket gateway scheme"),
    }

    if url.host_str().is_none() {
        bail!("websocket gateway address must include a host");
    }

    Ok(address.to_owned())
}

fn normalize_host_port_address(address: &str) -> Result<String> {
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

    let url = Url::parse(format!("ws://{address}").as_str())?;

    if !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        bail!("gateway address must be a host or host:port");
    }

    let host = url
        .host_str()
        .filter(|host| !host.trim().is_empty())
        .ok_or_else(|| anyhow::anyhow!("gateway address must include a host"))?;
    let port = url.port().unwrap_or(DEFAULT_GATEWAY_PORT);

    Ok(format_host_port(host, port))
}

fn resolve_addrs(listen_addr: &str) -> Result<Vec<SocketAddr>> {
    let socket_addr = resolve_socket_address_input(listen_addr)?;
    let addrs: Vec<SocketAddr> = socket_addr
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

fn resolve_socket_address_input(address: &str) -> Result<String> {
    let trimmed = address.trim();

    if trimmed.starts_with("ws://") || trimmed.starts_with("wss://") {
        let url = Url::parse(trimmed)?;
        let host = url
            .host_str()
            .filter(|host| !host.trim().is_empty())
            .ok_or_else(|| anyhow::anyhow!("websocket gateway address must include a host"))?;
        let port = url
            .port()
            .or_else(|| default_port_for_ws_scheme(url.scheme()));

        let Some(port) = port else {
            bail!("websocket gateway address must include a port");
        };

        return Ok(format_host_port(host, port));
    }

    normalize_host_port_address(trimmed)
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
