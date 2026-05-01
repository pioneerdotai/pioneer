use crate::error::ToolError;
use futures_util::StreamExt;
use reqwest::header::{
    ACCEPT, ACCEPT_LANGUAGE, CACHE_CONTROL, HeaderMap, HeaderValue, PRAGMA, USER_AGENT,
};
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::collections::HashMap;
use std::time::Duration;
use std::time::Instant;
use url::Url;

#[derive(Debug, Clone)]
pub(crate) struct BufferedHttpRequest {
    pub method: String,
    pub url: String,
    pub timeout_ms: u64,
    pub follow_redirects: bool,
    pub user_agent: Option<String>,
    pub headers: HashMap<String, String>,
    pub query: Option<JsonMap<String, JsonValue>>,
    pub body: Option<JsonValue>,
    pub max_bytes: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct BufferedHttpResponse {
    pub request_url: String,
    pub final_url: String,
    pub status_code: u16,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
    pub truncated: bool,
    pub elapsed_ms: u64,
}

pub(crate) fn build_http_client(
    timeout_ms: u64,
    follow_redirects: bool,
    user_agent: Option<&str>,
) -> Result<reqwest::Client, ToolError> {
    let mut headers = HeaderMap::new();
    let user_agent = user_agent
        .and_then(|value| HeaderValue::from_str(value).ok())
        .unwrap_or_else(|| HeaderValue::from_static("Mozilla/5.0"));
    headers.insert(USER_AGENT, user_agent);
    headers.insert(ACCEPT_LANGUAGE, HeaderValue::from_static("en-US,en;q=0.9"));
    headers.insert(
        ACCEPT,
        HeaderValue::from_static(
            "text/html,application/xhtml+xml,application/xml;q=0.9,text/plain;q=0.8,*/*;q=0.7",
        ),
    );
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    headers.insert(PRAGMA, HeaderValue::from_static("no-cache"));

    let redirect = if follow_redirects {
        reqwest::redirect::Policy::limited(10)
    } else {
        reqwest::redirect::Policy::none()
    };

    reqwest::Client::builder()
        .default_headers(headers)
        .timeout(Duration::from_millis(timeout_ms))
        .redirect(redirect)
        .build()
        .map_err(|error| ToolError::internal(format!("failed to build http client: {error}")))
}

pub(crate) async fn execute_buffered_http_request(
    request: BufferedHttpRequest,
) -> Result<BufferedHttpResponse, ToolError> {
    if request.max_bytes == 0 {
        return Err(ToolError::invalid_arguments(
            "http request max_bytes must be greater than 0",
        ));
    }

    validate_http_url(request.url.as_str())?;
    let client = build_http_client(
        request.timeout_ms.max(1),
        request.follow_redirects,
        request.user_agent.as_deref(),
    )?;

    let method_text = request.method.trim().to_uppercase();
    let method = reqwest::Method::from_bytes(method_text.as_bytes()).map_err(|error| {
        ToolError::invalid_arguments(format!("invalid HTTP method `{}`: {error}", request.method))
    })?;

    let mut outgoing = client.request(method, request.url.as_str());
    for (name, value) in request.headers {
        if !name.trim().is_empty() {
            outgoing = outgoing.header(name, value);
        }
    }

    if let Some(query) = request.query.as_ref() {
        outgoing = outgoing.query(query);
    }

    if let Some(body) = request.body.as_ref() {
        outgoing = outgoing.json(body);
    }

    let started = Instant::now();
    let response = outgoing
        .send()
        .await
        .map_err(|error| ToolError::execution_failed(format!("request failed: {error}")))?;

    let status_code = response.status().as_u16();
    let final_url = response.url().to_string();
    let headers = response
        .headers()
        .iter()
        .map(|(key, value)| {
            (
                key.as_str().to_owned(),
                value.to_str().unwrap_or_default().to_owned(),
            )
        })
        .collect::<HashMap<_, _>>();

    let mut stream = response.bytes_stream();
    let mut body = Vec::new();
    let mut truncated = false;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| {
            ToolError::execution_failed(format!("response stream error: {error}"))
        })?;

        if body.len() >= request.max_bytes {
            truncated = true;
            break;
        }

        let remaining = request.max_bytes.saturating_sub(body.len());
        if chunk.len() > remaining {
            body.extend_from_slice(&chunk[..remaining]);
            truncated = true;
            break;
        }

        body.extend_from_slice(&chunk);
    }

    Ok(BufferedHttpResponse {
        request_url: request.url,
        final_url,
        status_code,
        headers,
        body,
        truncated,
        elapsed_ms: started.elapsed().as_millis() as u64,
    })
}

pub(crate) fn validate_http_url(input: &str) -> Result<(), ToolError> {
    let parsed = Url::parse(input)
        .map_err(|error| ToolError::invalid_arguments(format!("invalid url `{input}`: {error}")))?;
    match parsed.scheme() {
        "http" | "https" => Ok(()),
        other => Err(ToolError::invalid_arguments(format!(
            "unsupported url scheme `{other}` (expected http or https)"
        ))),
    }
}
