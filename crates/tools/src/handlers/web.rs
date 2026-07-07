use super::http::{
    BufferedHttpRequest, build_http_client, execute_buffered_http_request, validate_http_url,
};
use crate::WebToolsConfig;
use crate::context::{FunctionToolOutput, ToolInvocation, ToolOutput, ToolPayload};
use crate::error::ToolError;
use crate::network_policy::enforce_network_url;
use crate::registry::ToolHandler;
use crate::web::{
    DownloadModelPayload, WebFetchLink, WebFetchModelPayload, WebFetchTruncation,
    WebSearchModelPayload, WebSearchResultItem, default_favicon_url, render_download_ui_text,
    render_web_fetch_ui_text, render_web_search_ui_text,
};
use crate::{FilePolicyChecker, FilePolicyDecision};
use async_trait::async_trait;
use futures_util::StreamExt;
use pioneer_protocol::TurnExecutionSecuritySnapshot;
use scraper::{Html, Selector};
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Instant;
use tokio::io::AsyncWriteExt;
use url::Url;

pub struct WebSearchHandler {
    config: WebToolsConfig,
}
pub struct WebFetchHandler {
    config: WebToolsConfig,
}
pub struct DownloadUrlHandler {
    config: WebToolsConfig,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ExtractMode {
    Markdown,
    Text,
    Raw,
    Auto,
}

impl Default for ExtractMode {
    fn default() -> Self {
        Self::Auto
    }
}

impl ExtractMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Markdown => "markdown",
            Self::Text => "text",
            Self::Raw => "raw",
            Self::Auto => "auto",
        }
    }
}

#[derive(Debug, Deserialize)]
struct WebSearchArgs {
    #[serde(default)]
    query: Option<String>,
    #[serde(default)]
    q: Option<String>,
    #[serde(default)]
    max_results: Option<usize>,
    #[serde(default)]
    region: Option<String>,
    #[serde(default)]
    safesearch: Option<String>,
    #[serde(default)]
    freshness: Option<String>,
    #[serde(default)]
    max_snippet_chars: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct WebFetchArgs {
    url: String,
    #[serde(default)]
    extract_mode: Option<ExtractMode>,
    #[serde(default)]
    timeout_ms: Option<u64>,
    #[serde(default)]
    max_bytes: Option<usize>,
    #[serde(default)]
    include_headers: Option<bool>,
    #[serde(default)]
    follow_redirects: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct DownloadUrlArgs {
    url: String,
    #[serde(default)]
    destination: Option<String>,
    #[serde(default)]
    overwrite: Option<bool>,
    #[serde(default)]
    create_dirs: Option<bool>,
    #[serde(default)]
    timeout_ms: Option<u64>,
    #[serde(default)]
    max_bytes: Option<usize>,
    #[serde(default)]
    follow_redirects: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
struct WebSearchResult {
    rank: usize,
    title: String,
    url: String,
    favicon: Option<String>,
    #[serde(skip_serializing_if = "String::is_empty")]
    snippet: String,
    source: String,
}

#[derive(Debug, Clone)]
struct HttpFetchResult {
    request_url: String,
    final_url: String,
    status_code: u16,
    content_type: Option<String>,
    headers: HashMap<String, String>,
    body: Vec<u8>,
    truncated: bool,
    elapsed_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
struct ExtractedContent {
    extract_mode: ExtractMode,
    output: String,
    output_truncated: bool,
    title: Option<String>,
    favicon: Option<String>,
    word_count: Option<usize>,
    links: Vec<WebFetchLink>,
    extractor: Option<String>,
}

#[derive(Debug, Clone)]
struct DownloadStreamResult {
    request_url: String,
    final_url: String,
    status_code: u16,
    content_type: Option<String>,
    bytes_written: u64,
    sha256: String,
    truncated: bool,
    elapsed_ms: u64,
}

#[derive(Debug, Clone, Copy)]
struct FetchSettings {
    timeout_ms: u64,
    max_bytes: usize,
    follow_redirects: bool,
    include_headers: bool,
}

impl WebSearchHandler {
    pub fn new(config: WebToolsConfig) -> Self {
        Self {
            config: config.normalized(),
        }
    }
}

impl WebFetchHandler {
    pub fn new(config: WebToolsConfig) -> Self {
        Self {
            config: config.normalized(),
        }
    }
}

impl DownloadUrlHandler {
    pub fn new(config: WebToolsConfig) -> Self {
        Self {
            config: config.normalized(),
        }
    }
}

#[async_trait]
impl ToolHandler for WebSearchHandler {
    async fn handle(
        &self,
        invocation: ToolInvocation,
        _trace: crate::events::ToolEventTrace,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let args = parse_json_args::<WebSearchArgs>(invocation.payload)?;

        let query = args.query.or(args.q).unwrap_or_default();
        if query.trim().is_empty() {
            return Err(ToolError::invalid_arguments(
                "web_search requires non-empty query or q",
            ));
        }

        let max_results = args
            .max_results
            .unwrap_or(self.config.default_max_results)
            .clamp(1, self.config.hard_max_results.max(1));

        let snippet_chars = args
            .max_snippet_chars
            .unwrap_or(self.config.default_snippet_chars)
            .clamp(64, self.config.hard_max_snippet_chars.max(64));

        let started = Instant::now();
        let mut results = search_duckduckgo(
            query.as_str(),
            max_results,
            args.region.as_deref(),
            args.safesearch.as_deref(),
            args.freshness.as_deref(),
            snippet_chars,
            &self.config,
            invocation.execution_security_snapshot.as_ref(),
        )
        .await?;

        if results.len() > max_results {
            results.truncate(max_results);
        }

        let typed_results = results
            .into_iter()
            .map(|item| WebSearchResultItem {
                rank: item.rank,
                title: item.title,
                url: item.url,
                favicon: item.favicon,
                snippet: item.snippet,
                source: item.source,
                published_at: None,
            })
            .collect::<Vec<_>>();

        let payload = WebSearchModelPayload {
            query,
            provider: "duckduckgo".to_owned(),
            took_ms: started.elapsed().as_millis() as u64,
            result_count: typed_results.len(),
            truncated: false,
            results: typed_results,
        };
        let output = render_web_search_ui_text(&payload);
        let payload_json = serde_json::to_value(&payload)
            .map_err(|error| ToolError::internal(format!("failed to encode payload: {error}")))?;

        Ok(Box::new(FunctionToolOutput::with_payload(
            output,
            true,
            payload_json,
        )))
    }
}

#[async_trait]
impl ToolHandler for WebFetchHandler {
    async fn handle(
        &self,
        invocation: ToolInvocation,
        _trace: crate::events::ToolEventTrace,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let args = parse_json_args::<WebFetchArgs>(invocation.payload)?;

        let settings = FetchSettings {
            timeout_ms: args
                .timeout_ms
                .unwrap_or(self.config.default_timeout_ms)
                .clamp(1, self.config.hard_max_timeout_ms.max(1)),
            max_bytes: args
                .max_bytes
                .unwrap_or(self.config.default_fetch_max_bytes)
                .clamp(1024, self.config.hard_fetch_max_bytes.max(1024)),
            follow_redirects: args.follow_redirects.unwrap_or(true),
            include_headers: args.include_headers.unwrap_or(false),
        };

        let fetched = fetch_url(
            args.url.as_str(),
            settings,
            self.config.default_user_agent.as_str(),
            invocation.execution_security_snapshot.as_ref(),
        )
        .await?;

        let requested_mode = args.extract_mode.unwrap_or_default();

        let extracted = extract_from_http(
            &fetched,
            requested_mode,
            self.config
                .default_link_count
                .clamp(1, self.config.hard_link_count.max(1)),
            self.config.default_render_max_chars,
        )?;

        let payload = WebFetchModelPayload {
            url: fetched.request_url.clone(),
            final_url: fetched.final_url.clone(),
            favicon: extracted.favicon.clone(),
            status_code: fetched.status_code,
            success: fetched.status_code < 400,
            content_type: fetched.content_type.clone(),
            extract_mode: requested_mode.as_str().to_owned(),
            resolved_mode: extracted.extract_mode.as_str().to_owned(),
            extractor: extracted.extractor.clone(),
            elapsed_ms: fetched.elapsed_ms,
            bytes_received: fetched.body.len(),
            truncated: WebFetchTruncation {
                network: fetched.truncated,
                output: extracted.output_truncated,
            },
            title: extracted.title.clone(),
            word_count: extracted.word_count,
            links: extracted.links.clone(),
            content: extracted.output.clone(),
        };
        let output = render_web_fetch_ui_text(&payload);
        let mut payload_json = serde_json::to_value(&payload)
            .map_err(|error| ToolError::internal(format!("failed to encode payload: {error}")))?;
        if settings.include_headers
            && let Some(map) = payload_json.as_object_mut()
        {
            map.insert(
                "headers".to_owned(),
                serde_json::to_value(&fetched.headers).unwrap_or_else(|_| serde_json::json!({})),
            );
        }

        Ok(Box::new(FunctionToolOutput::with_payload(
            output,
            fetched.status_code < 400,
            payload_json,
        )))
    }
}

#[async_trait]
impl ToolHandler for DownloadUrlHandler {
    async fn handle(
        &self,
        invocation: ToolInvocation,
        _trace: crate::events::ToolEventTrace,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let args = parse_json_args::<DownloadUrlArgs>(invocation.payload)?;

        let settings = FetchSettings {
            timeout_ms: args
                .timeout_ms
                .unwrap_or(self.config.default_timeout_ms)
                .clamp(1, self.config.hard_max_timeout_ms.max(1)),
            max_bytes: args
                .max_bytes
                .unwrap_or(self.config.default_download_max_bytes)
                .clamp(1024, self.config.hard_download_max_bytes.max(1024)),
            follow_redirects: args.follow_redirects.unwrap_or(true),
            include_headers: false,
        };

        let mut destination = resolve_download_destination(
            invocation.workdir.as_path(),
            args.destination.as_deref(),
            args.url.as_str(),
        )?;
        if let Some(snapshot) = invocation.execution_security_snapshot.as_ref() {
            match FilePolicyChecker::check_write(snapshot, destination.as_path()) {
                FilePolicyDecision::Allowed(grant) => {
                    destination = grant.resolved_path;
                }
                FilePolicyDecision::Denied(deny) => {
                    return Err(ToolError::Rejected(format!(
                        "filesystem sandbox denied Write for download destination `{}`: {}",
                        deny.requested_path.display(),
                        deny.message
                    )));
                }
            }
        }

        let stream_result = download_to_file(
            args.url.as_str(),
            destination.as_path(),
            settings,
            args.overwrite.unwrap_or(false),
            args.create_dirs.unwrap_or(true),
            self.config.default_user_agent.as_str(),
            invocation.execution_security_snapshot.as_ref(),
        )
        .await?;

        let payload = DownloadModelPayload {
            url: stream_result.request_url.clone(),
            final_url: stream_result.final_url.clone(),
            favicon: default_favicon_url(stream_result.final_url.as_str()),
            status_code: stream_result.status_code,
            success: stream_result.status_code < 400,
            path: destination.display().to_string(),
            bytes_written: stream_result.bytes_written,
            sha256: stream_result.sha256,
            content_type: stream_result.content_type,
            elapsed_ms: stream_result.elapsed_ms,
            truncated: stream_result.truncated,
        };
        let output = render_download_ui_text(&payload);
        let payload_json = serde_json::to_value(&payload)
            .map_err(|error| ToolError::internal(format!("failed to encode payload: {error}")))?;

        Ok(Box::new(FunctionToolOutput::with_payload(
            output,
            stream_result.status_code < 400,
            payload_json,
        )))
    }
}

fn parse_json_args<T: for<'de> Deserialize<'de>>(payload: ToolPayload) -> Result<T, ToolError> {
    match payload {
        ToolPayload::Function { arguments } => serde_json::from_value(arguments).map_err(|error| {
            ToolError::invalid_arguments(format!("failed to parse function arguments: {error}"))
        }),
        ToolPayload::Custom { input } => {
            serde_json::from_str::<T>(input.as_str()).map_err(|error| {
                ToolError::invalid_arguments(format!("failed to parse custom arguments: {error}"))
            })
        }
        other => Err(ToolError::invalid_arguments(format!(
            "unsupported payload for web tool: {}",
            other.log_payload()
        ))),
    }
}

async fn search_duckduckgo(
    query: &str,
    max_results: usize,
    region: Option<&str>,
    safesearch: Option<&str>,
    freshness: Option<&str>,
    max_snippet_chars: usize,
    config: &WebToolsConfig,
    security_snapshot: Option<&TurnExecutionSecuritySnapshot>,
) -> Result<Vec<WebSearchResult>, ToolError> {
    let direct = search_duckduckgo_html(
        query,
        max_results,
        region,
        safesearch,
        freshness,
        max_snippet_chars,
        config,
        security_snapshot,
    )
    .await;

    match direct {
        Ok(results) if !results.is_empty() => Ok(results),
        Ok(_) => {
            search_duckduckgo_instant_api(
                query,
                max_results,
                max_snippet_chars,
                config,
                security_snapshot,
            )
            .await
        }
        Err(_) => {
            search_duckduckgo_instant_api(
                query,
                max_results,
                max_snippet_chars,
                config,
                security_snapshot,
            )
            .await
        }
    }
}

async fn search_duckduckgo_html(
    query: &str,
    max_results: usize,
    region: Option<&str>,
    safesearch: Option<&str>,
    freshness: Option<&str>,
    max_snippet_chars: usize,
    config: &WebToolsConfig,
    security_snapshot: Option<&TurnExecutionSecuritySnapshot>,
) -> Result<Vec<WebSearchResult>, ToolError> {
    enforce_network_url(
        security_snapshot,
        config.ddg_html_search_url.as_str(),
        "web_search",
    )?;

    let timeout_ms = config
        .default_timeout_ms
        .clamp(1, config.hard_max_timeout_ms.max(1));
    let client = build_http_client(timeout_ms, true, Some(config.default_user_agent.as_str()))?;

    let mut params: Vec<(&str, String)> = vec![("q", query.to_owned())];
    if let Some(region) = region {
        if !region.trim().is_empty() {
            params.push(("kl", region.trim().to_owned()));
        }
    }

    if let Some(safesearch) = safesearch {
        let kp = match safesearch.trim().to_lowercase().as_str() {
            "off" => Some("-2"),
            "moderate" => Some("-1"),
            "strict" => Some("1"),
            _ => None,
        };
        if let Some(kp) = kp {
            params.push(("kp", kp.to_owned()));
        }
    }

    if let Some(freshness) = freshness {
        let df = match freshness.trim().to_lowercase().as_str() {
            "day" => Some("d"),
            "week" => Some("w"),
            "month" => Some("m"),
            "year" => Some("y"),
            _ => None,
        };
        if let Some(df) = df {
            params.push(("df", df.to_owned()));
        }
    }

    let response = client
        .get(config.ddg_html_search_url.as_str())
        .query(&params)
        .send()
        .await
        .map_err(|error| {
            ToolError::execution_failed(format!("duckduckgo request failed: {error}"))
        })?;

    let status = response.status().as_u16();
    let body = response.text().await.map_err(|error| {
        ToolError::execution_failed(format!("duckduckgo response decode failed: {error}"))
    })?;

    if status >= 400 {
        return Err(ToolError::execution_failed(format!(
            "duckduckgo returned HTTP {status}"
        )));
    }

    if body.contains("anomaly-modal")
        || body.contains("Unfortunately, bots use DuckDuckGo too")
        || body.contains("anomaly.js")
    {
        return Err(ToolError::execution_failed(
            "duckduckgo returned anti-bot challenge",
        ));
    }

    let mut results = parse_duckduckgo_html_results(body.as_str(), max_snippet_chars);
    if results.len() > max_results {
        results.truncate(max_results);
    }
    Ok(results)
}

fn parse_duckduckgo_html_results(body: &str, max_snippet_chars: usize) -> Vec<WebSearchResult> {
    let doc = Html::parse_document(body);

    let item_selector = Selector::parse("div.result").expect("valid selector");
    let title_selector = Selector::parse("a.result__a").expect("valid selector");
    let snippet_selector =
        Selector::parse("a.result__snippet, div.result__snippet").expect("valid selector");

    let mut results = Vec::new();
    for (idx, item) in doc.select(&item_selector).enumerate() {
        let Some(anchor) = item.select(&title_selector).next() else {
            continue;
        };

        let title = compact_whitespace(anchor.text().collect::<Vec<_>>().join(" "));
        if title.is_empty() {
            continue;
        }

        let raw_href = anchor.value().attr("href").unwrap_or_default();
        let url = normalize_duckduckgo_result_url(raw_href);
        if url.is_empty() {
            continue;
        }

        let snippet = item
            .select(&snippet_selector)
            .next()
            .map(|node| compact_whitespace(node.text().collect::<Vec<_>>().join(" ")))
            .map(|text| truncate_chars(text.as_str(), max_snippet_chars).0)
            .unwrap_or_default();
        let favicon = default_favicon_url(url.as_str());

        results.push(WebSearchResult {
            rank: idx.saturating_add(1),
            title,
            url,
            favicon,
            snippet,
            source: "duckduckgo_html".to_owned(),
        });
    }

    results
}

async fn search_duckduckgo_instant_api(
    query: &str,
    max_results: usize,
    max_snippet_chars: usize,
    config: &WebToolsConfig,
    security_snapshot: Option<&TurnExecutionSecuritySnapshot>,
) -> Result<Vec<WebSearchResult>, ToolError> {
    enforce_network_url(
        security_snapshot,
        config.ddg_instant_api_url.as_str(),
        "web_search",
    )?;

    let timeout_ms = config
        .default_timeout_ms
        .clamp(1, config.hard_max_timeout_ms.max(1));
    let client = build_http_client(timeout_ms, true, Some(config.default_user_agent.as_str()))?;
    let payload = client
        .get(config.ddg_instant_api_url.as_str())
        .query(&[
            ("q", query),
            ("format", "json"),
            ("no_html", "1"),
            ("skip_disambig", "1"),
        ])
        .send()
        .await
        .map_err(|error| {
            ToolError::execution_failed(format!("duckduckgo instant API request failed: {error}"))
        })?
        .json::<JsonValue>()
        .await
        .map_err(|error| {
            ToolError::execution_failed(format!("duckduckgo instant API decode failed: {error}"))
        })?;

    let mut results = Vec::new();

    if let (Some(url), Some(snippet)) = (
        payload
            .get("AbstractURL")
            .and_then(JsonValue::as_str)
            .filter(|value| value.starts_with("http")),
        payload.get("AbstractText").and_then(JsonValue::as_str),
    ) {
        let title = payload
            .get("Heading")
            .and_then(JsonValue::as_str)
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(query)
            .to_owned();
        results.push(WebSearchResult {
            rank: 1,
            title,
            url: url.to_owned(),
            favicon: default_favicon_url(url),
            snippet: truncate_chars(snippet, max_snippet_chars).0,
            source: "duckduckgo_instant_api".to_owned(),
        });
    }

    let mut push_result = |title: String, url: String, snippet: String| {
        if results.len() >= max_results {
            return;
        }
        if results.iter().any(|item| item.url == url) {
            return;
        }
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            return;
        }
        let favicon = default_favicon_url(url.as_str());
        results.push(WebSearchResult {
            rank: results.len().saturating_add(1),
            title,
            url,
            favicon,
            snippet: truncate_chars(snippet.as_str(), max_snippet_chars).0,
            source: "duckduckgo_instant_api".to_owned(),
        });
    };

    if let Some(array) = payload.get("Results").and_then(JsonValue::as_array) {
        for item in array {
            let url = item
                .get("FirstURL")
                .and_then(JsonValue::as_str)
                .unwrap_or_default()
                .to_owned();
            let text = item
                .get("Text")
                .and_then(JsonValue::as_str)
                .unwrap_or_default()
                .to_owned();
            if url.is_empty() || text.is_empty() {
                continue;
            }
            let title = text
                .split(" - ")
                .next()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or(text.as_str())
                .to_owned();
            push_result(title, url, text);
        }
    }

    if let Some(array) = payload.get("RelatedTopics").and_then(JsonValue::as_array) {
        collect_related_topics(array, &mut push_result);
    }

    if results.is_empty() {
        return Err(ToolError::execution_failed(
            "duckduckgo search returned no results",
        ));
    }

    if results.len() > max_results {
        results.truncate(max_results);
    }

    Ok(results)
}

fn collect_related_topics<F>(items: &[JsonValue], push_result: &mut F)
where
    F: FnMut(String, String, String),
{
    for item in items {
        if let Some(nested) = item.get("Topics").and_then(JsonValue::as_array) {
            collect_related_topics(nested, push_result);
            continue;
        }
        let url = item
            .get("FirstURL")
            .and_then(JsonValue::as_str)
            .unwrap_or_default()
            .to_owned();
        let text = item
            .get("Text")
            .and_then(JsonValue::as_str)
            .unwrap_or_default()
            .to_owned();
        if url.is_empty() || text.is_empty() {
            continue;
        }
        let title = text
            .split(" - ")
            .next()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or(text.as_str())
            .to_owned();
        push_result(title, url, text);
    }
}

fn normalize_duckduckgo_result_url(raw: &str) -> String {
    if raw.trim().is_empty() {
        return String::new();
    }

    let normalized = if raw.starts_with("//") {
        format!("https:{raw}")
    } else if raw.starts_with('/') {
        format!("https://duckduckgo.com{raw}")
    } else {
        raw.to_owned()
    };

    if let Ok(parsed) = Url::parse(normalized.as_str())
        && parsed.domain() == Some("duckduckgo.com")
        && parsed.path() == "/l/"
        && let Some(target) = parsed
            .query_pairs()
            .find(|(key, _)| key == "uddg")
            .map(|(_, value)| value.to_string())
    {
        return target;
    }

    normalized
}

async fn fetch_url(
    url: &str,
    settings: FetchSettings,
    user_agent: &str,
    security_snapshot: Option<&TurnExecutionSecuritySnapshot>,
) -> Result<HttpFetchResult, ToolError> {
    enforce_network_url(security_snapshot, url, "web_fetch")?;

    let response = execute_buffered_http_request(BufferedHttpRequest {
        method: "GET".to_owned(),
        url: url.to_owned(),
        timeout_ms: settings.timeout_ms,
        follow_redirects: settings.follow_redirects,
        user_agent: Some(user_agent.to_owned()),
        headers: HashMap::new(),
        query: None,
        body: None,
        max_bytes: settings.max_bytes.max(1),
    })
    .await?;

    let headers = response.headers;
    let content_type = headers
        .get("content-type")
        .cloned()
        .filter(|value| !value.trim().is_empty());

    Ok(HttpFetchResult {
        request_url: response.request_url,
        final_url: response.final_url,
        status_code: response.status_code,
        content_type,
        headers: if settings.include_headers {
            headers
        } else {
            HashMap::new()
        },
        body: response.body,
        truncated: response.truncated,
        elapsed_ms: response.elapsed_ms,
    })
}

async fn download_to_file(
    url: &str,
    destination: &Path,
    settings: FetchSettings,
    overwrite: bool,
    create_dirs: bool,
    user_agent: &str,
    security_snapshot: Option<&TurnExecutionSecuritySnapshot>,
) -> Result<DownloadStreamResult, ToolError> {
    validate_http_url(url)?;
    enforce_network_url(security_snapshot, url, "download_url")?;

    if create_dirs && let Some(parent) = destination.parent() {
        tokio::fs::create_dir_all(parent).await.map_err(|error| {
            ToolError::execution_failed(format!(
                "failed to create destination directory `{}`: {error}",
                parent.display()
            ))
        })?;
    }

    let mut options = tokio::fs::OpenOptions::new();
    options.write(true).create(true);
    if overwrite {
        options.truncate(true);
    } else {
        options.create_new(true);
    }

    let mut file = options.open(destination).await.map_err(|error| {
        if error.kind() == std::io::ErrorKind::AlreadyExists {
            ToolError::execution_failed(format!(
                "destination already exists: {} (set overwrite=true)",
                destination.display()
            ))
        } else {
            ToolError::execution_failed(format!(
                "failed to open destination `{}`: {error}",
                destination.display()
            ))
        }
    })?;

    let client = build_http_client(
        settings.timeout_ms,
        settings.follow_redirects,
        Some(user_agent),
    )?;
    let started = Instant::now();
    let response = client
        .get(url)
        .send()
        .await
        .map_err(|error| ToolError::execution_failed(format!("request failed: {error}")))?;

    let status_code = response.status().as_u16();
    if status_code >= 400 {
        let _ = tokio::fs::remove_file(destination).await;
        return Err(ToolError::execution_failed(format!(
            "download request failed with HTTP {status_code}"
        )));
    }

    let final_url = response.url().to_string();
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
        .filter(|value| !value.trim().is_empty());

    let mut stream = response.bytes_stream();
    let mut bytes_written: u64 = 0;
    let mut hasher = Sha256::new();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| {
            ToolError::execution_failed(format!("response stream error: {error}"))
        })?;
        let chunk_len = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
        let next_size = bytes_written.saturating_add(chunk_len);
        if next_size > u64::try_from(settings.max_bytes).unwrap_or(u64::MAX) {
            let _ = tokio::fs::remove_file(destination).await;
            return Err(ToolError::execution_failed(format!(
                "download exceeds max_bytes limit ({})",
                settings.max_bytes
            )));
        }

        file.write_all(chunk.as_ref()).await.map_err(|error| {
            ToolError::execution_failed(format!(
                "failed to write `{}`: {error}",
                destination.display()
            ))
        })?;
        bytes_written = next_size;
        hasher.update(chunk.as_ref());
    }

    file.flush().await.map_err(|error| {
        ToolError::execution_failed(format!(
            "failed to flush `{}`: {error}",
            destination.display()
        ))
    })?;

    Ok(DownloadStreamResult {
        request_url: url.to_owned(),
        final_url,
        status_code,
        content_type,
        bytes_written,
        sha256: hex::encode(hasher.finalize()),
        truncated: false,
        elapsed_ms: started.elapsed().as_millis() as u64,
    })
}

fn extract_from_http(
    fetched: &HttpFetchResult,
    requested_mode: ExtractMode,
    max_links: usize,
    max_output_chars: usize,
) -> Result<ExtractedContent, ToolError> {
    let content_type = fetched.content_type.as_deref().unwrap_or("").to_lowercase();

    let resolved_mode = if requested_mode == ExtractMode::Auto {
        if content_type.contains("html") || content_type.contains("xml") {
            ExtractMode::Markdown
        } else if content_type.starts_with("text/")
            || content_type.contains("json")
            || content_type.contains("javascript")
        {
            ExtractMode::Text
        } else {
            ExtractMode::Raw
        }
    } else {
        requested_mode
    };

    if content_type.contains("html")
        || content_type.contains("xhtml")
        || content_type.contains("xml")
        || looks_like_html(fetched.body.as_slice())
    {
        let html = String::from_utf8_lossy(fetched.body.as_slice()).to_string();
        let favicon = extract_favicon_from_html(html.as_str(), fetched.final_url.as_str())
            .or_else(|| default_favicon_url(fetched.final_url.as_str()));
        let extraction = webclaw_core::extract_with_options(
            html.as_str(),
            Some(fetched.final_url.as_str()),
            &webclaw_core::ExtractionOptions::default(),
        )
        .map_err(|error| ToolError::execution_failed(format!("web extraction failed: {error}")))?;

        let base = match resolved_mode {
            ExtractMode::Markdown | ExtractMode::Auto => extraction.content.markdown.clone(),
            ExtractMode::Text => {
                if extraction.content.plain_text.trim().is_empty() {
                    extraction.content.markdown.clone()
                } else {
                    extraction.content.plain_text.clone()
                }
            }
            ExtractMode::Raw => html,
        };

        let (output, output_truncated) = truncate_chars(base.as_str(), max_output_chars);
        let links = extraction
            .content
            .links
            .iter()
            .take(max_links)
            .enumerate()
            .map(|(index, link)| WebFetchLink {
                index,
                text: compact_whitespace(link.text.clone()),
                href: link.href.clone(),
            })
            .collect::<Vec<_>>();

        return Ok(ExtractedContent {
            extract_mode: resolved_mode,
            output,
            output_truncated,
            title: extraction.metadata.title,
            favicon,
            word_count: Some(extraction.metadata.word_count),
            links,
            extractor: Some("webclaw_core".to_owned()),
        });
    }

    let raw = if resolved_mode == ExtractMode::Raw {
        render_raw_bytes(fetched.body.as_slice(), fetched.content_type.as_deref())
    } else {
        String::from_utf8_lossy(fetched.body.as_slice()).to_string()
    };
    let (output, output_truncated) = truncate_chars(raw.as_str(), max_output_chars);

    Ok(ExtractedContent {
        extract_mode: resolved_mode,
        output,
        output_truncated,
        title: None,
        favicon: default_favicon_url(fetched.final_url.as_str()),
        word_count: Some(raw.split_whitespace().count()),
        links: Vec::new(),
        extractor: None,
    })
}

fn extract_favicon_from_html(html: &str, base_url: &str) -> Option<String> {
    let doc = Html::parse_document(html);
    let link_selector = Selector::parse("link[rel][href]").expect("valid selector");

    for link in doc.select(&link_selector) {
        let rel = link.value().attr("rel").unwrap_or_default().to_lowercase();
        if !rel.split_whitespace().any(|token| token.contains("icon")) {
            continue;
        }
        let href = link.value().attr("href").unwrap_or_default().trim();
        if href.is_empty() {
            continue;
        }
        if let Some(resolved) = resolve_http_like_url(base_url, href) {
            return Some(resolved);
        }
    }

    None
}

fn resolve_http_like_url(base_url: &str, href: &str) -> Option<String> {
    if href.trim().is_empty() {
        return None;
    }

    if let Ok(absolute) = Url::parse(href) {
        if matches!(absolute.scheme(), "http" | "https") {
            return Some(absolute.to_string());
        }
        return None;
    }

    let base = Url::parse(base_url).ok()?;
    if !matches!(base.scheme(), "http" | "https") {
        return None;
    }
    let joined = base.join(href).ok()?;
    if matches!(joined.scheme(), "http" | "https") {
        Some(joined.to_string())
    } else {
        None
    }
}

fn render_raw_bytes(bytes: &[u8], content_type: Option<&str>) -> String {
    if !is_probably_binary(content_type, bytes) {
        return String::from_utf8_lossy(bytes).to_string();
    }

    let preview = bytes
        .iter()
        .take(64)
        .map(|byte| format!("{:02x}", byte))
        .collect::<Vec<_>>()
        .join("");

    format!(
        "[binary content, {} bytes, hex preview: {}]",
        bytes.len(),
        preview
    )
}

fn is_probably_binary(content_type: Option<&str>, bytes: &[u8]) -> bool {
    if let Some(content_type) = content_type {
        let lowered = content_type.to_lowercase();
        if lowered.starts_with("text/")
            || lowered.contains("json")
            || lowered.contains("xml")
            || lowered.contains("javascript")
            || lowered.contains("html")
        {
            return false;
        }
        if lowered.starts_with("image/")
            || lowered.starts_with("audio/")
            || lowered.starts_with("video/")
            || lowered.contains("application/pdf")
            || lowered.contains("application/octet-stream")
        {
            return true;
        }
    }

    let probe = &bytes[..bytes.len().min(1024)];
    let mut non_printable = 0usize;
    for byte in probe {
        let printable = matches!(byte, b'\n' | b'\r' | b'\t' | 32..=126);
        if !printable {
            non_printable = non_printable.saturating_add(1);
        }
    }
    non_printable.saturating_mul(100) / probe.len().max(1) > 20
}

fn resolve_download_destination(
    workdir: &Path,
    destination: Option<&str>,
    source_url: &str,
) -> Result<PathBuf, ToolError> {
    let inferred_name = infer_filename_from_url(source_url);

    let path = match destination {
        Some(destination) if !destination.trim().is_empty() => {
            let destination = PathBuf::from(destination);
            if destination.is_absolute() {
                destination
            } else {
                workdir.join(destination)
            }
        }
        _ => workdir.join(inferred_name.as_str()),
    };

    if path
        .file_name()
        .and_then(|name| name.to_str())
        .map(|name| name.trim().is_empty())
        .unwrap_or(true)
    {
        return Err(ToolError::invalid_arguments(format!(
            "invalid destination path `{}`",
            path.display()
        )));
    }

    if path.is_dir() {
        return Ok(path.join(inferred_name.as_str()));
    }

    Ok(path)
}

fn infer_filename_from_url(final_url: &str) -> String {
    if let Ok(url) = Url::parse(final_url)
        && let Some(segment) = url.path_segments().and_then(|segments| segments.last())
        && !segment.trim().is_empty()
    {
        return sanitize_filename(segment);
    }
    "download.bin".to_owned()
}

fn sanitize_filename(raw: &str) -> String {
    let candidate = raw
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_') {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();

    if candidate.is_empty() {
        "download.bin".to_owned()
    } else {
        candidate
    }
}

fn truncate_chars(text: &str, max_chars: usize) -> (String, bool) {
    if max_chars == 0 {
        return (String::new(), !text.is_empty());
    }
    let total_chars = text.chars().count();
    if total_chars <= max_chars {
        return (text.to_owned(), false);
    }

    let mut out = String::new();
    for (index, ch) in text.chars().enumerate() {
        if index >= max_chars {
            break;
        }
        out.push(ch);
    }
    out.push_str("\n... [output truncated]");
    (out, true)
}

fn compact_whitespace(input: String) -> String {
    input.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn looks_like_html(bytes: &[u8]) -> bool {
    let probe = String::from_utf8_lossy(&bytes[..bytes.len().min(1024)]).to_lowercase();
    probe.contains("<html") || probe.contains("<!doctype html") || probe.contains("<body")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_duckduckgo_redirect_extracts_uddg() {
        let raw = "https://duckduckgo.com/l/?kh=-1&uddg=https%3A%2F%2Fexample.com%2Fpath%3Fa%3D1";
        assert_eq!(
            normalize_duckduckgo_result_url(raw),
            "https://example.com/path?a=1"
        );
    }

    #[test]
    fn binary_detection_prefers_known_binary_content_types() {
        assert!(is_probably_binary(
            Some("application/pdf"),
            b"%PDF-1.7 binary payload"
        ));
    }

    #[test]
    fn extract_favicon_resolves_relative_icon_href() {
        let html = r#"<html><head><link rel="icon" href="/assets/favicon.png"></head></html>"#;
        let favicon = extract_favicon_from_html(html, "https://example.com/docs/page")
            .expect("favicon must be extracted");
        assert_eq!(favicon, "https://example.com/assets/favicon.png");
    }

    #[test]
    fn default_favicon_uses_origin() {
        let favicon = default_favicon_url("https://example.com/path?q=1")
            .expect("fallback favicon should be generated");
        assert_eq!(favicon, "https://example.com/favicon.ico");
    }

    #[tokio::test]
    async fn network_policy_web_fetch_denies_disabled_snapshot_before_request() {
        let snapshot = pioneer_protocol::TurnExecutionSecuritySnapshot::read_only(
            pioneer_protocol::TurnPermissionProfileSnapshot::from_mode(
                pioneer_protocol::TurnPermissionMode::Supervised,
                pioneer_protocol::TurnPermissionProfileSource::Composer,
            ),
            "/tmp/workspace",
            Vec::new(),
            1_700_000_000_000,
        );
        let settings = FetchSettings {
            timeout_ms: 1,
            max_bytes: 1024,
            follow_redirects: false,
            include_headers: false,
        };

        let error = fetch_url(
            "https://example.com",
            settings,
            "pioneer-test",
            Some(&snapshot),
        )
        .await
        .expect_err("disabled network should reject before request");

        assert!(
            matches!(error, ToolError::Rejected(ref message) if message.contains("network access is disabled")),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn download_url_policy_denies_destination_outside_workspace_before_request() {
        let base =
            std::env::temp_dir().join(format!("pioneer-download-policy-{}", std::process::id()));
        let workspace = base.join("workspace");
        let outside = base.join("outside");
        std::fs::create_dir_all(workspace.as_path()).expect("workspace should create");
        std::fs::create_dir_all(outside.as_path()).expect("outside should create");
        let destination = outside.join("archive.tgz");
        let snapshot = pioneer_protocol::TurnExecutionSecuritySnapshot::read_only(
            pioneer_protocol::TurnPermissionProfileSnapshot::from_mode(
                pioneer_protocol::TurnPermissionMode::Supervised,
                pioneer_protocol::TurnPermissionProfileSource::Composer,
            ),
            workspace.to_string_lossy(),
            vec![
                pioneer_protocol::TurnFilesystemSandboxEntry::workspace_root(
                    pioneer_protocol::TurnFilesystemAccess::Read,
                    workspace.to_string_lossy(),
                ),
            ],
            1_700_000_000_000,
        );
        let handler = DownloadUrlHandler::new(test_web_config());
        let invocation = ToolInvocation {
            call_id: "call_download".to_owned(),
            tool_name: "download_url".to_owned(),
            source: crate::context::ToolCallSource::Model,
            payload: ToolPayload::Function {
                arguments: serde_json::json!({
                    "url": "https://example.com/archive.tgz",
                    "destination": destination.display().to_string(),
                }),
            },
            workdir: workspace.clone(),
            environment: Default::default(),
            attempt_id: 1,
            idempotency_key: None,
            recovery: crate::spec::ToolRecoveryMetadata::default(),
            permission_metadata: crate::spec::ToolPermissionMetadata::default(),
            execution_security_snapshot: Some(snapshot),
            cancellation: tokio_util::sync::CancellationToken::new(),
        };

        let result = handler
            .handle(
                invocation,
                crate::events::ToolEventBus::default().start_trace(
                    "turn",
                    "call_download",
                    "download_url",
                ),
            )
            .await;
        let error = match result {
            Ok(_) => panic!("outside destination should be rejected before network"),
            Err(error) => error,
        };

        assert!(
            matches!(error, ToolError::Rejected(ref message) if message.contains("download destination") && message.contains("outside the allowed sandbox roots")),
            "unexpected error: {error}"
        );
        assert!(!destination.exists());
        let _ = std::fs::remove_dir_all(base);
    }

    fn test_web_config() -> WebToolsConfig {
        WebToolsConfig {
            default_timeout_ms: 1,
            hard_max_timeout_ms: 10,
            default_fetch_max_bytes: 1024,
            hard_fetch_max_bytes: 2048,
            default_download_max_bytes: 1024,
            hard_download_max_bytes: 2048,
            default_max_results: 3,
            hard_max_results: 5,
            default_snippet_chars: 120,
            hard_max_snippet_chars: 240,
            default_link_count: 3,
            hard_link_count: 5,
            default_render_max_chars: 1024,
            ddg_html_search_url: "https://duckduckgo.com/html/".to_owned(),
            ddg_instant_api_url: "https://api.duckduckgo.com/".to_owned(),
            default_user_agent: "Mozilla/5.0".to_owned(),
        }
    }
}
