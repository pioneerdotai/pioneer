use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CanonicalMcpToolResult {
    #[serde(default)]
    pub(crate) content: JsonValue,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) structured_content: Option<JsonValue>,
    pub(crate) is_error: bool,
    pub(crate) duration_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) meta: Option<JsonValue>,
}
