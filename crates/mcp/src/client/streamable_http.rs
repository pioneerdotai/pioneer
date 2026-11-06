use crate::runtime::{MaterializedHttpTransport, McpRuntimeError};
use http::{HeaderName, HeaderValue};
use rmcp::transport::{
    StreamableHttpClientTransport, streamable_http_client::StreamableHttpClientTransportConfig,
};
use std::collections::HashMap;

pub(crate) fn build_streamable_http_transport(
    transport: &MaterializedHttpTransport,
    client: crate::oauth::ManagedHttpClient,
) -> Result<StreamableHttpClientTransport<crate::oauth::ManagedHttpClient>, McpRuntimeError> {
    let portable = client.installation.is_portable_plugin();
    let headers = configured_headers(&transport.headers, portable)?;

    let config = StreamableHttpClientTransportConfig::with_uri(transport.url.clone())
        .custom_headers(headers)
        .reinit_on_expired_session(true);

    Ok(StreamableHttpClientTransport::with_client(client, config))
}

fn configured_headers(
    configured: &std::collections::BTreeMap<String, String>,
    portable: bool,
) -> Result<HashMap<HeaderName, HeaderValue>, McpRuntimeError> {
    let mut headers = HashMap::new();
    for (name, value) in configured {
        // The SDK rejects some conflicting custom headers instead of replacing
        // them. Portable packages require the client's generated headers to win.
        if portable
            && matches!(
                name.to_ascii_lowercase().as_str(),
                "accept"
                    | "content-type"
                    | "content-length"
                    | "host"
                    | "connection"
                    | "transfer-encoding"
                    | "mcp-session-id"
                    | "mcp-protocol-version"
                    | "last-event-id"
            )
        {
            continue;
        }
        let name = HeaderName::from_bytes(name.as_bytes()).map_err(|error| {
            McpRuntimeError::failed(format!("invalid HTTP header name `{name}`: {error}"))
        })?;
        let value = HeaderValue::from_str(value).map_err(|error| {
            McpRuntimeError::failed(format!("invalid HTTP header value for `{name}`: {error}"))
        })?;
        headers.insert(name, value);
    }

    Ok(headers)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn portable_headers_defer_to_client_generated_headers_while_legacy_is_unchanged() {
        let values = std::collections::BTreeMap::from([
            ("Accept".into(), "custom".into()),
            ("CONTENT-TYPE".into(), "custom".into()),
            ("Mcp-Session-Id".into(), "custom".into()),
            ("X-Context".into(), "${PLUGIN_ROOT}".into()),
        ]);
        let portable = configured_headers(&values, true).unwrap();
        assert_eq!(portable.len(), 1);
        assert_eq!(
            portable[&HeaderName::from_static("x-context")],
            "${PLUGIN_ROOT}"
        );
        let legacy = configured_headers(&values, false).unwrap();
        assert_eq!(legacy.len(), 4);
    }
}
