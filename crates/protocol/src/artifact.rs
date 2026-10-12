use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use zeroize::Zeroize;

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactKind {
    File,
    Text,
    Image,
    Audio,
    Video,
    Pdf,
    Spreadsheet,
    Archive,
    Json,
    GeneratedImage,
    Screenshot,
    WorkspaceFile,
    DirectoryManifest,
    Unknown,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactStatus {
    Ready,
    Pending,
    Quarantined,
    Deleted,
    MissingExternalSource,
    Failed,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactCreatedByKind {
    User,
    Agent,
    Tool,
    Task,
    System,
    Import,
    ExternalAgent,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactBindingKind {
    UserInput,
    AgentOutput,
    ToolOutput,
    TaskResult,
    TaskResultCandidate,
    ContextAttachment,
    DerivedFrom,
    Preview,
    ManualAttach,
    DraftUpload,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactBindingDirection {
    Input,
    Output,
    Context,
    Derived,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactRole {
    User,
    Assistant,
    Tool,
    System,
    Task,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactProjectionKind {
    PlainText,
    Thumbnail,
    JsonSummary,
    PdfText,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactProjectionStatus {
    Pending,
    Ready,
    Failed,
    Stale,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactViewGrantDisposition {
    Inline,
    Attachment,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactUploadSourceKind {
    UserComposer,
    DragDrop,
    Paste,
    Api,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactPrepareKind {
    Image,
    Document,
    Data,
    Archive,
    Code,
    Log,
    Other,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactPrepareParams {
    pub display_name: String,
    pub kind: ArtifactPrepareKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactPrepareResponse {
    pub output_path: String,
    pub output_dir: String,
    pub expires_at: String,
    pub display_name: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactRegisterParams {
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<ArtifactPrepareKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prepared_output_path: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactRegisterResponse {
    pub artifact_id: String,
    pub version_id: String,
    pub display_name: String,
    pub kind: ArtifactKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    pub size_bytes: u64,
    pub sha256: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ArtifactPreviewRef {
    pub projection_kind: ArtifactProjectionKind,
    pub status: ArtifactProjectionStatus,
    pub artifact_id: String,
    pub version_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blob_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ArtifactRef {
    pub artifact_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
    pub display_name: String,
    pub kind: ArtifactKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    pub status: ArtifactStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview: Option<ArtifactPreviewRef>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ArtifactBindingSummary {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
    pub binding_id: String,
    pub workspace_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_item_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_run_id: Option<String>,
    pub binding_kind: ArtifactBindingKind,
    pub direction: ArtifactBindingDirection,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_index: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<ArtifactRole>,
    pub created_at: i64,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct ArtifactSummary {
    pub artifact: ArtifactRef,
    pub workspace_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_thread_id: Option<String>,
    pub created_by_kind: ArtifactCreatedByKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by_actor_id: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bindings: Vec<ArtifactBindingSummary>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, serde_json::Value>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ArtifactCapabilitiesParams {
    pub workspace_id: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ArtifactCapabilitiesResponse {
    pub upload: ArtifactUploadCapabilities,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ArtifactUploadCapabilities {
    pub required_for_local_paths: bool,
    pub recommended_chunk_size_bytes: u64,
    pub max_chunk_size_bytes: u64,
    pub max_file_size_bytes: u64,
    pub max_files_per_turn: u64,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
pub struct ArtifactListParams {
    pub workspace_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub kinds: Vec<ArtifactKind>,
    #[serde(default)]
    pub include_deleted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
}

pub type ArtifactListForThreadParams = ArtifactListParams;
pub type ArtifactListForTurnParams = ArtifactListParams;
pub type ArtifactListForMessageParams = ArtifactListParams;

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct ArtifactListResponse {
    #[serde(default)]
    pub items: Vec<ArtifactSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ArtifactGetParams {
    pub workspace_id: String,
    pub artifact_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct ArtifactGetResponse {
    pub artifact: ArtifactSummary,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ArtifactViewGrantCreateParams {
    #[schemars(length(min = 1, max = 128))]
    pub workspace_id: String,
    #[schemars(length(min = 1, max = 128))]
    pub artifact_id: String,
    #[schemars(length(min = 1, max = 128))]
    pub version_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projection_kind: Option<ArtifactProjectionKind>,
    pub disposition: ArtifactViewGrantDisposition,
}

#[derive(Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ArtifactViewGrantCreateResponse {
    #[schemars(length(min = 58, max = 58))]
    pub relative_url: String,
    pub expires_at: u64,
}

impl std::fmt::Debug for ArtifactViewGrantCreateResponse {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ArtifactViewGrantCreateResponse")
            .field("relative_url", &"[redacted]")
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

impl Drop for ArtifactViewGrantCreateResponse {
    fn drop(&mut self) {
        self.relative_url.zeroize();
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ArtifactUploadStartParams {
    pub workspace_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub planned_turn_id: Option<String>,
    pub client_attachment_id: String,
    pub file_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    pub size_bytes: u64,
    pub sha256: String,
    pub source_kind: ArtifactUploadSourceKind,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ArtifactUploadStartResponse {
    pub upload_id: String,
    pub recommended_chunk_size_bytes: u64,
    pub max_chunk_size_bytes: u64,
    pub max_size_bytes: u64,
    pub expires_at_unix: i64,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ArtifactUploadChunkHeader {
    pub workspace_id: String,
    pub upload_id: String,
    pub offset: u64,
    pub len: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunk_sha256: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ArtifactUploadChunkAckNotification {
    pub workspace_id: String,
    pub upload_id: String,
    pub offset: u64,
    pub len: u64,
    pub received_bytes: u64,
    pub next_offset: u64,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ArtifactUploadFinishParams {
    pub workspace_id: String,
    pub upload_id: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ArtifactUploadFinishResponse {
    pub upload_id: String,
    pub artifact: ArtifactRef,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ArtifactUploadAbortParams {
    pub workspace_id: String,
    pub upload_id: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ArtifactUploadAbortResponse {
    pub upload_id: String,
    pub status: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ArtifactBindParams {
    pub workspace_id: String,
    pub artifact_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_item_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_run_id: Option<String>,
    pub binding_kind: ArtifactBindingKind,
    pub direction: ArtifactBindingDirection,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<ArtifactRole>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_index: Option<i64>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ArtifactBindResponse {
    pub binding: ArtifactBindingSummary,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ArtifactDeleteParams {
    pub workspace_id: String,
    pub artifact_id: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ArtifactDeleteResponse {
    pub artifact_id: String,
    pub status: ArtifactStatus,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ArtifactRestoreParams {
    pub workspace_id: String,
    pub artifact_id: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ArtifactRestoreResponse {
    pub artifact_id: String,
    pub status: ArtifactStatus,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct ArtifactCreatedNotification {
    pub workspace_id: String,
    pub artifact: ArtifactSummary,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct ArtifactUpdatedNotification {
    pub workspace_id: String,
    pub artifact: ArtifactSummary,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ArtifactDeletedNotification {
    pub workspace_id: String,
    pub artifact_id: String,
    pub status: ArtifactStatus,
    pub deleted_at: i64,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ThreadArtifactsChangedNotification {
    pub workspace_id: String,
    pub thread_id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifact_ids: Vec<String>,
    pub reason: String,
    pub generated_at: i64,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ArtifactProjectionUpdatedNotification {
    pub workspace_id: String,
    pub artifact_id: String,
    pub version_id: String,
    pub projection_kind: ArtifactProjectionKind,
    pub status: ArtifactProjectionStatus,
    pub updated_at: i64,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ArtifactUploadProgressNotification {
    pub workspace_id: String,
    pub upload_id: String,
    pub received_bytes: u64,
    pub total_size_bytes: u64,
    pub next_offset: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constants;
    use serde_json::json;

    fn artifact_ref() -> ArtifactRef {
        ArtifactRef {
            artifact_id: "art_123".to_owned(),
            version_id: Some("av_123".to_owned()),
            display_name: "report.pdf".to_owned(),
            kind: ArtifactKind::Pdf,
            mime_type: Some("application/pdf".to_owned()),
            size_bytes: Some(42),
            sha256: Some("a".repeat(64)),
            status: ArtifactStatus::Ready,
            preview: None,
        }
    }

    #[test]
    fn artifact_ref_round_trips_with_snake_case_enums() {
        let encoded = serde_json::to_value(artifact_ref()).expect("artifact ref encode");

        assert_eq!(encoded["kind"], json!("pdf"));
        assert_eq!(encoded["status"], json!("ready"));
        assert_eq!(encoded["artifact_id"], json!("art_123"));
        assert_eq!(encoded["version_id"], json!("av_123"));

        let decoded: ArtifactRef = serde_json::from_value(encoded).expect("artifact ref decode");
        assert_eq!(decoded, artifact_ref());
    }

    #[test]
    fn upload_chunk_header_round_trips() {
        let upload = ArtifactUploadChunkHeader {
            workspace_id: "ws_1".to_owned(),
            upload_id: "up_1".to_owned(),
            offset: 10,
            len: 5,
            chunk_sha256: Some("b".repeat(64)),
        };
        let upload_json = serde_json::to_value(&upload).expect("upload header encode");
        assert_eq!(upload_json["workspace_id"], json!("ws_1"));
        assert_eq!(upload_json["chunk_sha256"], json!("b".repeat(64)));
        assert_eq!(
            serde_json::from_value::<ArtifactUploadChunkHeader>(upload_json)
                .expect("upload header decode"),
            upload
        );
    }

    #[test]
    fn constants_include_artifact_methods_and_events() {
        assert_eq!(constants::methods::ARTIFACT_LIST, "artifact/list");
        assert_eq!(
            constants::methods::ARTIFACT_VIEW_GRANT_CREATE,
            "artifact/view_grant/create"
        );
        assert_eq!(
            constants::methods::ARTIFACT_LIST_FOR_THREAD,
            "artifact/list/thread"
        );
        assert_eq!(
            constants::methods::ARTIFACT_LIST_FOR_TURN,
            "artifact/list/turn"
        );
        assert_eq!(
            constants::methods::ARTIFACT_LIST_FOR_MESSAGE,
            "artifact/list/message"
        );
        assert_eq!(
            constants::methods::ARTIFACT_UPLOAD_START,
            "artifact/upload/start"
        );
        assert_eq!(constants::events::ARTIFACT_CREATED, "artifact/created");
        assert_eq!(
            constants::events::THREAD_ARTIFACTS_CHANGED,
            "thread/artifacts/changed"
        );
        assert_eq!(
            constants::events::ARTIFACT_PROJECTION_UPDATED,
            "artifact/projection/updated"
        );
        assert_eq!(
            constants::events::ARTIFACT_UPLOAD_PROGRESS,
            "artifact/upload/progress"
        );
    }

    #[test]
    fn view_grant_contract_requires_exact_version_and_relative_url() {
        let params = ArtifactViewGrantCreateParams {
            workspace_id: "workspace-1".to_owned(),
            artifact_id: "artifact-1".to_owned(),
            version_id: "version-1".to_owned(),
            projection_kind: Some(ArtifactProjectionKind::Thumbnail),
            disposition: ArtifactViewGrantDisposition::Inline,
        };
        let encoded = serde_json::to_value(&params).unwrap();
        assert_eq!(encoded["version_id"], "version-1");
        assert_eq!(encoded["disposition"], "inline");
        assert_eq!(
            serde_json::from_value::<ArtifactViewGrantCreateParams>(encoded).unwrap(),
            params
        );

        let response = ArtifactViewGrantCreateResponse {
            relative_url: format!("/storage/views/{}", "a".repeat(43)),
            expires_at: 1_180,
        };
        let rendered = format!("{response:?}");
        assert!(!rendered.contains(response.relative_url.as_str()));
        assert!(rendered.contains("[redacted]"));
        let response_json = serde_json::to_value(&response).unwrap();
        assert!(
            response_json["relative_url"]
                .as_str()
                .unwrap()
                .starts_with("/storage/views/")
        );
        assert_eq!(response_json["expires_at"], 1_180);
    }

    #[test]
    fn schema_documents_include_artifact_contracts() {
        let schema_names = crate::protocol_schema_documents()
            .into_iter()
            .map(|document| document.file_name)
            .collect::<Vec<_>>();

        for expected in [
            "artifact_ref.json",
            "artifact_summary.json",
            "artifact_list_params.json",
            "artifact_list_response.json",
            "artifact_upload_start_params.json",
            "artifact_upload_chunk_header.json",
            "artifact_upload_chunk_ack_notification.json",
            "artifact_created_notification.json",
        ] {
            assert!(
                schema_names.iter().any(|name| *name == expected),
                "missing schema document {expected}"
            );
        }
    }
}

/// An exact Gateway-created HTML publication. Generic HTML attachments never
/// acquire this descriptor based on MIME alone.
#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HtmlPublication {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub call_id: String,
    pub title: String,
    pub artifact: ArtifactRef,
}
impl HtmlPublication {
    pub fn from_summary(summary: &ArtifactSummary, thread: &str, turn: &str) -> Option<Self> {
        if summary.created_by_kind != ArtifactCreatedByKind::Tool
            || summary.created_by_actor_id.as_deref() != Some("html_render")
            || summary.artifact.mime_type.as_deref() != Some("text/html")
            || summary
                .artifact
                .version_id
                .as_ref()
                .is_none_or(|v| v.is_empty())
            || summary
                .artifact
                .sha256
                .as_ref()
                .is_none_or(|v| v.len() != 64 || !v.bytes().all(|c| c.is_ascii_hexdigit()))
        {
            return None;
        }
        let marker = summary.metadata.get("htmlTool")?;
        if marker["version"] != 1 || marker["publication"] != true || marker["turnId"] != turn {
            return None;
        }
        let call = marker["callId"].as_str()?;
        let title = marker["title"].as_str()?;
        if call.is_empty()
            || !(1..=200).contains(&title.chars().count())
            || marker["index"] != "document"
            || summary.primary_thread_id.as_deref() != Some(thread)
        {
            return None;
        }
        if !summary.bindings.iter().any(|b| {
            b.thread_id.as_deref() == Some(thread)
                && b.turn_id.as_deref() == Some(turn)
                && b.tool_call_id.as_deref() == Some(call)
                && b.version_id.as_ref() == summary.artifact.version_id.as_ref()
                && b.binding_kind == ArtifactBindingKind::ToolOutput
                && b.direction == ArtifactBindingDirection::Output
                && b.role == Some(ArtifactRole::Tool)
        }) {
            return None;
        }
        Some(Self {
            workspace_id: summary.workspace_id.clone(),
            thread_id: thread.into(),
            turn_id: turn.into(),
            call_id: call.into(),
            title: title.into(),
            artifact: summary.artifact.clone(),
        })
    }
}

/// Compact timeline lifecycle snapshots. Canonical provider inputs remain in the
/// existing history store; exact HTML/screenshot bytes live in artifact versions.
pub fn compact_html_timeline_item(item: &mut crate::TurnItem) {
    if let crate::TurnItem::DynamicToolCall {
        tool_name,
        arguments,
        ..
    } = item
    {
        if matches!(tool_name.as_str(), "html_render" | "html_preview") {
            // Native lifecycle parsing wraps incomplete JSON in {"raw": ...}.
            // Treat that wrapper like a partial argument string, without
            // changing canonical arguments or non-HTML tool projections.
            if let Some(partial) = arguments
                .as_str()
                .or_else(|| arguments.get("raw").and_then(serde_json::Value::as_str))
            {
                *arguments = serde_json::json!({"argumentsCharacters":partial.chars().count()});
            } else {
                compact_html_timeline_value(arguments);
            }
        }
    }
}

pub fn compact_html_timeline_value(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(object) => {
            if let Some(html) = object
                .remove("html")
                .and_then(|v| v.as_str().map(|v| v.chars().count()))
            {
                object.insert("htmlCharacters".into(), html.into());
            }
            if object.get("type").and_then(|v| v.as_str()) == Some("image") {
                object.remove("data");
            }
            for value in object.values_mut() {
                compact_html_timeline_value(value);
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                compact_html_timeline_value(value);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod html_projection_tests {
    use super::*;
    #[test]
    fn html_timeline_compaction_keeps_exact_refs_and_discards_document_and_image_bytes() {
        let mut value = serde_json::json!({"arguments":{"html":"<p>private source</p>","title":"Page"},"result":{"content":[{"type":"image","mimeType":"image/png","data":"encoded-bytes"}],"structuredContent":{"source":{"artifact_id":"source","version_id":"source-version"},"previews":[{"artifact":{"artifact_id":"shot","version_id":"shot-version"}}]}}});
        compact_html_timeline_value(&mut value);
        let serialized = value.to_string();
        assert!(!serialized.contains("private source"));
        assert!(!serialized.contains("encoded-bytes"));
        assert!(serialized.contains("source-version"));
        assert!(serialized.contains("shot-version"));
        assert_eq!(value["arguments"]["title"], "Page");
    }
}
