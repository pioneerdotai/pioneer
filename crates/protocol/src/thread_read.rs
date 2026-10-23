//! Observation-only agent tools. Workspace and user identity are server-owned.
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ThreadsListParams {
    pub limit: u32,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ThreadsHistoryReadParams {
    pub thread_id: String,
    /// Initial context around an exact saved turn or item; mutually exclusive with cursor.
    #[serde(default)]
    pub anchor: Option<ThreadHistoryAnchor>,
    #[serde(default)]
    pub direction: ThreadHistoryDirection,
    pub limit: u32,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ThreadsChildrenListParams {
    pub thread_id: String,
    pub recursive: bool,
    pub limit: u32,
    #[serde(default)]
    pub cursor: Option<String>,
}

/// Exact saved source in the requested thread. Omitting itemId anchors the turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ThreadHistoryAnchor {
    pub turn_id: String,
    #[serde(default)]
    pub item_id: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ThreadHistoryDirection {
    #[default]
    Older,
    Newer,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ThreadsSearchParams {
    #[schemars(length(min = 1, max = 4096))]
    pub query: String,
    #[schemars(range(min = 1, max = 20))]
    pub limit: u32,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ThreadsSearchLimitations {
    pub selection_limited: bool,
    pub coverage_limited: bool,
    pub semantic_approximate: bool,
    pub semantic_degraded: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ThreadsSearchResponse {
    pub threads: Vec<ThreadSearchCard>,
    pub next_cursor: Option<String>,
    pub eof: bool,
    pub limitations: ThreadsSearchLimitations,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ThreadSearchCard {
    pub thread_id: String,
    pub title: Option<String>,
    pub fragments: Vec<ThreadSearchFragment>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ThreadSearchFragment {
    pub snippet: String,
    pub source: ThreadSearchSource,
    pub provenance: ThreadSearchProvenance,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ThreadSearchSource {
    pub thread_id: String,
    pub anchor: ThreadHistoryAnchor,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ThreadSearchProvenance {
    pub actor_role: crate::ThreadEpisodicSourceActorRole,
    pub source_context: crate::ThreadEpisodicSourceContext,
}
