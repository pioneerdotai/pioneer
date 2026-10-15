use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WebSearchResultItem {
    pub rank: usize,
    pub title: String,
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub favicon: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub snippet: String,
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub published_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebSearchModelPayload {
    pub query: String,
    pub provider: String,
    pub took_ms: u64,
    pub result_count: usize,
    pub truncated: bool,
    pub results: Vec<WebSearchResultItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WebFetchTruncation {
    pub network: bool,
    pub output: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WebFetchLink {
    pub index: usize,
    pub text: String,
    pub href: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebFetchModelPayload {
    pub url: String,
    pub final_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub favicon: Option<String>,
    pub status_code: u16,
    pub success: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    pub extract_mode: String,
    pub resolved_mode: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extractor: Option<String>,
    pub elapsed_ms: u64,
    pub bytes_received: usize,
    pub truncated: WebFetchTruncation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub word_count: Option<usize>,
    pub links: Vec<WebFetchLink>,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadModelPayload {
    pub url: String,
    pub final_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub favicon: Option<String>,
    pub status_code: u16,
    pub success: bool,
    pub path: String,
    pub bytes_written: u64,
    pub sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    pub elapsed_ms: u64,
    pub truncated: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub durability_warning: Option<String>,
}

pub fn render_web_search_ui_text(payload: &WebSearchModelPayload) -> String {
    let mut lines = vec![
        format!("Query: {}", payload.query),
        format!("Provider: {}", payload.provider),
        format!("Results: {}", payload.result_count),
    ];

    if payload.took_ms > 0 {
        lines.push(format!("Duration: {}ms", payload.took_ms));
    }

    lines.push(String::new());

    for item in payload.results.iter().take(8) {
        lines.push(format!("{}. {}", item.rank, item.title));
        lines.push(item.url.clone());
        if !item.snippet.is_empty() {
            lines.push(item.snippet.clone());
        }
        lines.push(String::new());
    }

    if payload.truncated {
        lines.push("... [results truncated]".to_owned());
    }

    lines.join("\n")
}

pub fn render_web_fetch_ui_text(payload: &WebFetchModelPayload) -> String {
    let mut lines = vec![
        format!("URL: {}", payload.final_url),
        format!("Status: {}", payload.status_code),
        format!("Extract Mode: {}", payload.resolved_mode),
    ];
    if let Some(content_type) = payload.content_type.as_deref() {
        lines.push(format!("Content-Type: {content_type}"));
    }
    if let Some(title) = payload.title.as_deref()
        && !title.trim().is_empty()
    {
        lines.push(format!("Title: {title}"));
    }
    if let Some(word_count) = payload.word_count {
        lines.push(format!("Word Count: {word_count}"));
    }
    lines.push(format!("Bytes: {}", payload.bytes_received));
    if payload.elapsed_ms > 0 {
        lines.push(format!("Duration: {}ms", payload.elapsed_ms));
    }
    if payload.truncated.network || payload.truncated.output {
        lines.push(format!(
            "Truncated: network={} output={}",
            payload.truncated.network, payload.truncated.output
        ));
    }

    lines.push(String::new());
    lines.push(payload.content.clone());
    lines.join("\n")
}

pub fn render_download_ui_text(payload: &DownloadModelPayload) -> String {
    let mut lines = vec![
        format!("Downloaded: {}", payload.final_url),
        format!("Saved To: {}", payload.path),
        format!("Bytes: {}", payload.bytes_written),
        format!("Status: {}", payload.status_code),
        format!("SHA256: {}", payload.sha256),
    ];
    if let Some(content_type) = payload.content_type.as_deref() {
        lines.push(format!("Content-Type: {content_type}"));
    }
    if payload.elapsed_ms > 0 {
        lines.push(format!("Duration: {}ms", payload.elapsed_ms));
    }
    if payload.truncated {
        lines.push("Truncated: true".to_owned());
    }
    lines.join("\n")
}

pub fn default_favicon_url(page_url: &str) -> Option<String> {
    let mut parsed = Url::parse(page_url).ok()?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return None;
    }

    parsed.set_path("/favicon.ico");
    parsed.set_query(None);
    parsed.set_fragment(None);
    Some(parsed.to_string())
}
