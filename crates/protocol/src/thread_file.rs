use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use zeroize::Zeroize;

/// Maximum size of a workspace text file that may be opened by a remote client.
/// Files are rejected as a whole when they exceed this limit; responses are
/// never silently truncated.
pub const THREAD_FILE_VIEW_MAX_BYTES: u64 = 10 * 1024 * 1024;

#[derive(Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ThreadFileViewGrantCreateParams {
    #[schemars(length(min = 1, max = 128))]
    pub thread_id: String,
    #[schemars(length(min = 1, max = 128))]
    pub turn_id: String,
    #[schemars(length(min = 1, max = 128))]
    pub item_id: String,
    #[schemars(length(min = 1, max = 4096))]
    pub href: String,
}

impl std::fmt::Debug for ThreadFileViewGrantCreateParams {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ThreadFileViewGrantCreateParams")
            .field("thread_id", &self.thread_id)
            .field("turn_id", &self.turn_id)
            .field("item_id", &self.item_id)
            .field("href", &"[redacted]")
            .finish()
    }
}

impl Drop for ThreadFileViewGrantCreateParams {
    fn drop(&mut self) {
        self.href.zeroize();
    }
}

#[derive(Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ThreadFileViewGrantCreateResponse {
    #[schemars(length(min = 58, max = 58))]
    pub relative_url: String,
    pub expires_at: u64,
    #[schemars(length(min = 1, max = 255))]
    pub file_name: String,
    #[schemars(length(min = 1, max = 255))]
    pub content_type: String,
    #[schemars(range(max = 10485760))]
    pub size_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column: Option<u32>,
}

impl std::fmt::Debug for ThreadFileViewGrantCreateResponse {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ThreadFileViewGrantCreateResponse")
            .field("relative_url", &"[redacted]")
            .field("expires_at", &self.expires_at)
            .field("file_name", &self.file_name)
            .field("content_type", &self.content_type)
            .field("size_bytes", &self.size_bytes)
            .field("line", &self.line)
            .field("column", &self.column)
            .finish()
    }
}

impl Drop for ThreadFileViewGrantCreateResponse {
    fn drop(&mut self) {
        self.relative_url.zeroize();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn thread_file_grant_contract_round_trips_without_disclosing_secrets() {
        let params = ThreadFileViewGrantCreateParams {
            thread_id: "thread-1".to_owned(),
            turn_id: "turn-1".to_owned(),
            item_id: "item-1".to_owned(),
            href: "/workspace/private/main.rs:42:7".to_owned(),
        };
        let encoded = serde_json::to_value(&params).expect("params should serialize");
        assert_eq!(encoded["href"], "/workspace/private/main.rs:42:7");
        let debug = format!("{params:?}");
        assert!(debug.contains("[redacted]"));
        assert!(!debug.contains("/workspace/private"));

        let response = ThreadFileViewGrantCreateResponse {
            relative_url: format!("/storage/views/{}", "a".repeat(43)),
            expires_at: 1_700_000_000,
            file_name: "main.rs".to_owned(),
            content_type: "text/x-rust; charset=utf-8".to_owned(),
            size_bytes: 128,
            line: Some(42),
            column: Some(7),
        };
        let encoded = serde_json::to_value(&response).expect("response should serialize");
        assert_eq!(encoded["size_bytes"], 128);
        let debug = format!("{response:?}");
        assert!(debug.contains("[redacted]"));
        assert!(!debug.contains("/storage/views/"));
    }
}
