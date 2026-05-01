use super::*;

pub(super) fn build_ws_request(url: &str, token: Option<&str>) -> Result<Request<()>> {
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

pub(super) fn normalize_ws_url(address: &str) -> String {
    let trimmed = address.trim();
    if trimmed.starts_with("ws://") || trimmed.starts_with("wss://") {
        trimmed.to_owned()
    } else {
        format!("ws://{trimmed}")
    }
}
