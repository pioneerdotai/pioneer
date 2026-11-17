//! Conversation search contracts, independent of the retrieval backend.
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ThreadsSearchParams {
    /// Nonblank natural-language query; no maximum length is imposed.
    #[schemars(length(min = 1))]
    pub query: String,
    #[schemars(range(min = 1, max = 100))]
    pub limit: u32,
    #[serde(default)]
    pub cursor: Option<String>,
}

impl ThreadsSearchParams {
    /// Validate page size and nonblank query without imposing a query length limit.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.query.trim().is_empty() {
            return Err("query must not be empty");
        }
        if !(1..=100).contains(&self.limit) {
            return Err("limit must be between 1 and 100");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ThreadsSearchResponse {
    pub threads: Vec<ThreadsSearchThread>,
    pub next_cursor: Option<String>,
    pub eof: bool,
    pub coverage_incomplete: bool,
    pub warnings: Vec<String>,
    /// Fragment of one oversized serialized conversation card. Concatenate text
    /// until complete, following nextCursor, then continue the global result page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_fragment: Option<ThreadsSearchPageFragment>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ThreadsSearchThread {
    pub thread_id: String,
    pub title: Option<String>,
    pub fragments: Vec<ThreadsSearchFragment>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ThreadsSearchFragment {
    pub text: String,
    pub score: Option<f32>,
    pub source: ThreadsSearchSource,
}

/// Canonical application source; itemId is absent for sources without a saved item.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ThreadsSearchSource {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_id: Option<String>,
    pub source_kind: String,
    pub index_item_id: String,
    pub source_version: String,
}

/// Exact serialized-card continuation, bound to a currently readable source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ThreadsSearchPageFragment {
    pub text: String,
    pub source: ThreadsSearchSource,
    pub version: String,
    pub complete: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn search_response_preserves_nullable_title_and_oversized_card_continuation() {
        let source = serde_json::json!({"workspaceId":"w", "threadId":"t", "turnId":"u",
            "itemId":"i", "indexItemId":"indexed", "sourceKind":"user_message", "sourceVersion":"hash"});
        let value = serde_json::json!({
            "threads":[{"threadId":"t","title":null,"fragments":[{"text":"match","score":0.5,"source":source}]}],
            "pageFragment":{"text":"partial serialized card", "source":source,"version":"card-hash","complete":false},
            "nextCursor":"opaque", "eof":false,"coverageIncomplete":false,"warnings":[]
        });
        let response: ThreadsSearchResponse = serde_json::from_value(value.clone()).unwrap();
        assert!(response.threads[0].title.is_none());
        assert_eq!(
            response
                .page_fragment
                .as_ref()
                .unwrap()
                .source
                .source_version,
            "hash"
        );
        assert_eq!(serde_json::to_value(response).unwrap(), value);
    }

    #[test]
    fn search_input_rejects_blank_queries_and_invalid_pages_but_accepts_long_queries() {
        for (query, limit, valid) in [
            (" ".to_owned(), 1, false),
            ("query".to_owned(), 0, false),
            ("query".to_owned(), 101, false),
            ("query".to_owned(), 1, true),
            ("я".repeat(100_000), 100, true),
        ] {
            let params = ThreadsSearchParams {
                query,
                limit,
                cursor: None,
            };
            assert_eq!(params.validate().is_ok(), valid);
        }
        assert!(
            serde_json::from_value::<ThreadsSearchParams>(serde_json::json!({
                "query":"q", "limit":1, "principalId":"model-supplied"
            }))
            .is_err()
        );
    }
}
