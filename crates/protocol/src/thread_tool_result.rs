use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ThreadToolResultCursor {
    pub version: String,
    /// Unicode scalar offset in the source, not a UTF-8 byte offset.
    pub offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ThreadToolResultReadParams {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    #[serde(default)]
    pub cursor: Option<ThreadToolResultCursor>,
    #[serde(default)]
    pub max_tokens: Option<u64>,
    #[serde(default)]
    pub max_bytes: Option<u64>,
}

/// A Unicode text fragment of the saved canonical result JSON, including any
/// upstream truncation markers and attachment references. Cursor is source-version bound.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ThreadToolResultReadResponse {
    pub text: String,
    pub next: Option<ThreadToolResultCursor>,
    pub eof: bool,
}
