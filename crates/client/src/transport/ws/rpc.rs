//! WebSocket JSON-RPC primitives.

use anyhow::{Result, anyhow};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::{HeaderValue, Request};

pub fn build_ws_request(url: &str, token: Option<&str>) -> Result<Request<()>> {
    let mut request = url
        .into_client_request()
        .map_err(|error| anyhow!("failed to prepare websocket request: {error}"))?;

    if let Some(token) = token.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then_some(trimmed)
    }) {
        let header = HeaderValue::from_str(format!("Bearer {token}").as_str())
            .map_err(|error| anyhow!("invalid authorization header value: {error}"))?;
        request.headers_mut().insert("authorization", header);
    }

    Ok(request)
}

pub fn normalize_ws_url(address: &str) -> String {
    let trimmed = address.trim();
    if trimmed.starts_with("ws://") || trimmed.starts_with("wss://") {
        trimmed.to_owned()
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
}
