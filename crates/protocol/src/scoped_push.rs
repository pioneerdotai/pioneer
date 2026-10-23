//! Opt-in, connection-owned delivery of absolute sidebar and inbox values.
//!
//! Epochs are opaque demand identities. The first accepted packet is a snapshot
//! at revision one; a gap or recovery marker requires a fresh authoritative
//! bootstrap. Revisions are meaningful only inside that epoch and workspace.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct WorkspacePushParams {
    pub workspace_id: String,
    pub epoch: String,
    pub tree: bool,
    pub inbox: bool,
    /// Known metadata identities; indicators never discover or create threads.
    #[serde(default)]
    pub thread_ids: Vec<String>,
    /// Loaded inbox identities, used to reconcile membership at bootstrap.
    #[serde(default)]
    pub run_ids: Vec<String>,
    /// Existing exact keyset frontier; recovery preserves this loaded range.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inbox_cursor: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct WorkspacePushResponse {
    pub supported: bool,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ThreadIndicatorPatch {
    pub thread_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unread_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub has_running_tasks: Option<bool>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskInboxPatch {
    /// Authoritative inbox replacement, also used by a read-all mutation ack.
    #[serde(default)]
    pub replace: bool,
    pub upserts: Vec<crate::TaskCompletionInboxEntry>,
    pub removes: Vec<String>,
    pub unread_count: u64,
    /// Authoritative keyset frontier when snapshot or replace is true.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ThreadMetadataPatch {
    /// Canonical thin directory metadata; never a full timeline/history snapshot.
    pub thread: crate::Thread,
    /// True only when directory projection actually shortened the preview.
    /// A real shorter/empty edit is authoritative when this is false.
    pub preview_truncated: bool,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct WorkspacePushNotification {
    pub workspace_id: String,
    pub epoch: String,
    pub revision: u64,
    pub snapshot: bool,
    pub resync_required: bool,
    /// Compact metadata and indicators share one revision and atomic reduction.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub thread_summaries: Vec<ThreadMetadataPatch>,
    pub indicators: Vec<ThreadIndicatorPatch>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inbox: Option<TaskInboxPatch>,
}
