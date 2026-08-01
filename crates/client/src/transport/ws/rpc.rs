//! WebSocket JSON-RPC request construction.

use anyhow::{Result, anyhow};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::{HeaderValue, Request};
use zeroize::Zeroizing;

use crate::gateway::endpoint::{
    GatewayBaseUrl, PIONEER_PROTOCOL_VERSION, PIONEER_PROTOCOL_VERSION_HEADER,
};

pub fn build_ws_request(
    gateway_base_url: &GatewayBaseUrl,
    token: Option<&str>,
) -> Result<Request<()>> {
    let ws_url = gateway_base_url.websocket_url();
    let mut request = ws_url
        .as_str()
        .into_client_request()
        .map_err(|error| anyhow!("failed to prepare websocket request: {error}"))?;
    request.headers_mut().insert(
        PIONEER_PROTOCOL_VERSION_HEADER,
        HeaderValue::from_static(PIONEER_PROTOCOL_VERSION),
    );

    if let Some(token) = token.and_then(non_empty) {
        let authorization_value = Zeroizing::new(format!("Bearer {token}"));
        let header = HeaderValue::from_str(authorization_value.as_str())
            .map_err(|error| anyhow!("invalid authorization header value: {error}"))?;
        request.headers_mut().insert("authorization", header);
    }

    Ok(request)
}

fn non_empty(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then_some(trimmed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_targets_base_root_and_has_exact_fixed_protocol_header() {
        let base = GatewayBaseUrl::parse_presentation("https://relay.example/pioneer").unwrap();
        let request = build_ws_request(&base, Some(" token ")).unwrap();

        assert_eq!(request.uri().to_string(), "wss://relay.example/pioneer/");
        assert_eq!(
            request
                .headers()
                .get_all(PIONEER_PROTOCOL_VERSION_HEADER)
                .iter()
                .count(),
            1
        );
        assert_eq!(
            request
                .headers()
                .get(PIONEER_PROTOCOL_VERSION_HEADER)
                .and_then(|value| value.to_str().ok()),
            Some("1")
        );
        assert_eq!(
            request
                .headers()
                .get("authorization")
                .and_then(|value| value.to_str().ok()),
            Some("Bearer token")
        );
    }

    #[test]
    fn connector_has_no_legacy_path_or_endpoint_fallback_input() {
        let base = GatewayBaseUrl::parse_presentation("http://192.0.2.1:17878").unwrap();
        let request = build_ws_request(&base, None).unwrap();
        assert_eq!(request.uri().to_string(), "ws://192.0.2.1:17878/");
    }
}
