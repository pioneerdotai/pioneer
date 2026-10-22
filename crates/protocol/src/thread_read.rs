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
