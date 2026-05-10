use std::collections::BTreeMap;
use std::path::PathBuf;

use pioneer_protocol::{
    ArtifactBindingDirection, ArtifactBindingKind, ArtifactCreatedByKind, ArtifactKind,
    ArtifactRole, ArtifactSummary,
};

#[derive(Debug, Clone, PartialEq)]
pub struct IngestArtifactBytesRequest {
    pub workspace_id: String,
    pub primary_thread_id: Option<String>,
    pub bytes: Vec<u8>,
    pub display_name: String,
    pub kind: ArtifactKind,
    pub mime_type: Option<String>,
    pub created_by_kind: ArtifactCreatedByKind,
    pub created_by_actor_id: Option<String>,
    pub binding: Option<ArtifactBindingTarget>,
    pub metadata: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IngestArtifactTempFileRequest {
    pub workspace_id: String,
    pub primary_thread_id: Option<String>,
    pub temp_path: PathBuf,
    pub display_name: String,
    pub kind: ArtifactKind,
    pub mime_type: Option<String>,
    pub created_by_kind: ArtifactCreatedByKind,
    pub created_by_actor_id: Option<String>,
    pub binding: Option<ArtifactBindingTarget>,
    pub metadata: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactBindingTarget {
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub message_id: Option<String>,
    pub turn_item_id: Option<String>,
    pub tool_call_id: Option<String>,
    pub task_id: Option<String>,
    pub task_run_id: Option<String>,
    pub binding_kind: ArtifactBindingKind,
    pub direction: ArtifactBindingDirection,
    pub role: Option<ArtifactRole>,
    pub item_index: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BindArtifactRequest {
    pub workspace_id: String,
    pub artifact_id: String,
    pub version_id: Option<String>,
    pub target: ArtifactBindingTarget,
    pub metadata: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ArtifactListFilter {
    pub limit: Option<u64>,
    pub cursor: Option<String>,
    pub include_deleted: bool,
    pub kinds: Vec<ArtifactKind>,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub message_id: Option<String>,
    pub task_id: Option<String>,
    pub task_run_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ArtifactListPage {
    pub items: Vec<ArtifactSummary>,
    pub next_cursor: Option<String>,
}
