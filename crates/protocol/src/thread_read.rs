//! Observation-only agent tools. Workspace and user identity are server-owned.
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ThreadsListParams {
    /// Page size in creation order (created_at DESC, id DESC), not activity order.
    pub limit: u32,
    /// Opaque position after the last examined candidate, including skipped rows.
    /// Retain the same parameters; unrelated activity does not invalidate it.
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
