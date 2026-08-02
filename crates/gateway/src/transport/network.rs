use std::net::{IpAddr, SocketAddr};

use axum::http::{HeaderMap, HeaderName};

use crate::request_context::RequestNetworkContext;

const FORWARDED: HeaderName = HeaderName::from_static("forwarded");
const X_FORWARDED_FOR: HeaderName = HeaderName::from_static("x-forwarded-for");

pub(crate) fn resolve_request_network(
    peer: SocketAddr,
    headers: &HeaderMap,
    trusted_proxy_peers: &[IpAddr],
) -> RequestNetworkContext {
    if !trusted_proxy_peers.contains(&peer.ip()) {
        return RequestNetworkContext::direct(peer);
    }

    let forwarded = single_header(headers, &FORWARDED).and_then(parse_forwarded);
    let x_forwarded_for = single_header(headers, &X_FORWARDED_FOR).and_then(parse_single_ip);
    let client_ip = match (forwarded, x_forwarded_for) {
        (Some(client_ip), None) | (None, Some(client_ip)) => client_ip,
        // Conflicting forwarding mechanisms and malformed/multi-hop values are
        // intentionally ignored. The immediate peer remains the safe limit key.
        _ => return RequestNetworkContext::direct(peer),
    };

    RequestNetworkContext::TrustedProxy { client_ip }
}

fn single_header<'a>(headers: &'a HeaderMap, name: &HeaderName) -> Option<&'a str> {
    let mut values = headers.get_all(name).iter();
    let value = values.next()?.to_str().ok()?;
    values.next().is_none().then_some(value.trim())
}

fn parse_single_ip(value: &str) -> Option<IpAddr> {
    if value.is_empty() || value.contains(',') {
        return None;
    }
    value.parse().ok()
}

fn parse_forwarded(value: &str) -> Option<IpAddr> {
    if value.is_empty() || value.contains(',') {
        return None;
    }
    let mut found = None;
    for parameter in value.split(';') {
        let Some((name, value)) = parameter.trim().split_once('=') else {
            return None;
        };
        if !name.trim().eq_ignore_ascii_case("for") {
            continue;
        }
        if found.is_some() {
            return None;
        }
        found = parse_forwarded_for_value(value.trim());
        found?;
    }
    found
}

fn parse_forwarded_for_value(value: &str) -> Option<IpAddr> {
    let value = value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .unwrap_or(value);
    if let Some(ipv6) = value.strip_prefix('[').and_then(|value| value.strip_suffix(']')) {
        return ipv6.parse().ok();
    }
    parse_single_ip(value)
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;

    use super::*;

    fn peer(ip: [u8; 4]) -> SocketAddr {
        SocketAddr::from((ip, 17_878))
    }

    #[test]
    fn untrusted_peer_cannot_spoof_forwarded_address() {
        let direct = peer([203, 0, 113, 42]);
        let mut headers = HeaderMap::new();
        headers.insert(X_FORWARDED_FOR, HeaderValue::from_static("192.0.2.42"));
        headers.insert(FORWARDED, HeaderValue::from_static("for=192.0.2.62"));

        let network = resolve_request_network(direct, &headers, &[IpAddr::from([127, 0, 0, 1])]);
        assert_eq!(network.client_ip(), Some(direct.ip()));
        assert_eq!(network.safe_source(), "direct_peer");
    }

    #[test]
    fn trusted_peer_accepts_one_sanitized_forwarding_mechanism() {
        let proxy = peer([127, 0, 0, 1]);
        let trusted = [proxy.ip()];

        let mut xff = HeaderMap::new();
        xff.insert(X_FORWARDED_FOR, HeaderValue::from_static("192.0.2.42"));
        let network = resolve_request_network(proxy, &xff, &trusted);
        assert_eq!(network.client_ip(), Some(IpAddr::from([192, 0, 2, 42])));
        assert_eq!(network.safe_source(), "trusted_proxy");

        let mut forwarded = HeaderMap::new();
        forwarded.insert(
            FORWARDED,
            HeaderValue::from_static("for=2001:db8::42;proto=https"),
        );
        let network = resolve_request_network(proxy, &forwarded, &trusted);
        assert_eq!(network.client_ip(), Some("2001:db8::42".parse().unwrap()));
    }

    #[test]
    fn ambiguous_malformed_and_multi_hop_values_fail_back_to_immediate_peer() {
        let proxy = peer([127, 0, 0, 1]);
        let trusted = [proxy.ip()];
        for value in ["192.0.2.1, 192.0.2.2", "unknown", "", "192.0.2.1:443"] {
            let mut headers = HeaderMap::new();
            headers.insert(X_FORWARDED_FOR, HeaderValue::from_str(value).unwrap());
            assert_eq!(
                resolve_request_network(proxy, &headers, &trusted).client_ip(),
                Some(proxy.ip())
            );
        }

        let mut both = HeaderMap::new();
        both.insert(X_FORWARDED_FOR, HeaderValue::from_static("192.0.2.42"));
        both.insert(FORWARDED, HeaderValue::from_static("for=192.0.2.42"));
        assert_eq!(
            resolve_request_network(proxy, &both, &trusted).client_ip(),
            Some(proxy.ip())
        );
    }
}
