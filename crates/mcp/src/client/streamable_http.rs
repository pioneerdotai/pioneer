use crate::runtime::{MaterializedHttpTransport, McpRuntimeError};
use http::{HeaderName, HeaderValue};
use rmcp::transport::{
    StreamableHttpClientTransport, streamable_http_client::StreamableHttpClientTransportConfig,
};
use std::collections::HashMap;

pub fn build_streamable_http_transport(
    transport: &MaterializedHttpTransport,
) -> Result<StreamableHttpClientTransport<reqwest_0_13::Client>, McpRuntimeError> {
    let mut headers = HashMap::new();
    for (name, value) in &transport.headers {
        let name = HeaderName::from_bytes(name.as_bytes()).map_err(|error| {
            McpRuntimeError::failed(format!("invalid HTTP header name `{name}`: {error}"))
        })?;
        let value = HeaderValue::from_str(value).map_err(|error| {
            McpRuntimeError::failed(format!("invalid HTTP header value for `{name}`: {error}"))
        })?;
        headers.insert(name, value);
    }

    let config = StreamableHttpClientTransportConfig::with_uri(transport.url.clone())
        .custom_headers(headers)
        .reinit_on_expired_session(true);

    Ok(StreamableHttpClientTransport::from_config(config))
}
