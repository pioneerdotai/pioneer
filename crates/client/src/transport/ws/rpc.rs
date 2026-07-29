//! WebSocket JSON-RPC primitives.

use anyhow::{Result, anyhow};
use std::net::IpAddr;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::{HeaderValue, Request};
use url::Url;
use zeroize::Zeroizing;

pub fn build_ws_request(url: &str, token: Option<&str>) -> Result<Request<()>> {
    let token = token.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then_some(trimmed)
    });
    if token.is_some() && !authenticated_ws_url_is_allowed(url) {
        return Err(anyhow!(
            "plaintext authenticated transport is allowed only for loopback endpoints"
        ));
    }
    let mut request = url
        .into_client_request()
        .map_err(|error| anyhow!("failed to prepare websocket request: {error}"))?;

    if let Some(token) = token {
        let authorization_value = Zeroizing::new(format!("Bearer {token}"));
        let header = HeaderValue::from_str(authorization_value.as_str())
            .map_err(|error| anyhow!("invalid authorization header value: {error}"))?;
        request.headers_mut().insert("authorization", header);
    }

    Ok(request)
}

pub(crate) fn authenticated_ws_url_is_allowed(value: &str) -> bool {
    let Ok(url) = Url::parse(value) else {
        return false;
    };
    if !url.username().is_empty()
        || url.password().is_some()
        || url.host_str().is_none()
        || url.fragment().is_some()
    {
        return false;
    }
    match url.scheme() {
        "wss" => true,
        "ws" => url.host_str().is_some_and(|host| {
            host.eq_ignore_ascii_case("localhost")
                || host
                    .parse::<IpAddr>()
                    .is_ok_and(|address| address.is_loopback())
        }),
        _ => false,
    }
}

pub fn normalize_ws_url(address: &str) -> String {
    let trimmed = address.trim();
    if trimmed.starts_with("ws://") || trimmed.starts_with("wss://") {
        trimmed.to_owned()
    } else if let Some(rest) = trimmed.strip_prefix("https://") {
        format!("wss://{rest}")
    } else {
        format!("ws://{trimmed}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_ws_normalize_ws_url_keeps_existing_scheme() {
        assert_eq!(normalize_ws_url("ws://0.0.0.0:17878"), "ws://0.0.0.0:17878");
        assert_eq!(
            normalize_ws_url("wss://gateway.example.com/socket"),
            "wss://gateway.example.com/socket"
        );
    }

    #[test]
    fn transport_ws_normalize_ws_url_adds_ws_scheme_when_missing() {
        assert_eq!(normalize_ws_url("0.0.0.0:17878"), "ws://0.0.0.0:17878");
        assert_eq!(
            normalize_ws_url(" gateway.example.com:443 "),
            "ws://gateway.example.com:443"
        );
    }

    #[test]
    fn transport_ws_normalize_ws_url_maps_https_to_wss() {
        assert_eq!(
            normalize_ws_url("https://gateway.example.com"),
            "wss://gateway.example.com"
        );
    }

    #[test]
    fn transport_ws_request_adds_bearer_token() {
        let request = build_ws_request("ws://localhost:17878", Some(" token ")).expect("request");

        assert_eq!(
            request
                .headers()
                .get("authorization")
                .and_then(|value| value.to_str().ok()),
            Some("Bearer token")
        );
    }

    #[test]
    fn transport_ws_rejects_remote_plaintext_before_building_authorization() {
        let error = build_ws_request("ws://192.0.2.1:17878", Some("access-secret"))
            .expect_err("remote plaintext auth must fail");
        assert!(error.to_string().contains("loopback"));
        assert!(!error.to_string().contains("access-secret"));
        assert!(build_ws_request("wss://gateway.example", Some("access-secret")).is_ok());
        assert!(build_ws_request("ws://localhost:17878", Some("access-secret")).is_ok());
        assert!(
            build_ws_request("wss://user:password@gateway.example", Some("access-secret")).is_err()
        );
        assert!(
            build_ws_request(
                "wss://gateway.example/socket#access-secret",
                Some("access-secret")
            )
            .is_err()
        );
        assert!(build_ws_request("ws://192.0.2.1:17878", None).is_ok());
    }
}
