use crate::constants;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ThreadAgentsDocStatus {
    Draft,
    Active,
    Archived,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ThreadAgentsDocSaveReason {
    Autosave,
    Manual,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ThreadAgentsDocPayload {
    pub id: String,
    pub workspace_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub folder_id: Option<String>,
    pub status: ThreadAgentsDocStatus,
    pub title: String,
    pub content: String,
    pub content_sha256: String,
    pub version: i64,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ThreadAgentsDocSummary {
    pub id: String,
    pub workspace_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub folder_id: Option<String>,
    pub status: ThreadAgentsDocStatus,
    pub content_sha256: String,
    pub version: i64,
    pub char_count: usize,
    pub updated_at: i64,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ThreadAgentsDocResolvedPayload {
    pub doc: ThreadAgentsDocPayload,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_folder_id: Option<String>,
    #[serde(default)]
    pub source_path: Vec<String>,
    pub inherited: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_for_folder_id: Option<String>,
    pub resolved_at: i64,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
pub struct ThreadAgentsDocGetParams {
    pub workspace_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub folder_id: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
pub struct ThreadAgentsDocGetResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub explicit: Option<ThreadAgentsDocPayload>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective: Option<ThreadAgentsDocResolvedPayload>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ThreadAgentsDocSaveParams {
    pub workspace_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub folder_id: Option<String>,
    pub content: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_version: Option<i64>,
    #[serde(default = "default_save_reason")]
    pub save_reason: ThreadAgentsDocSaveReason,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ThreadAgentsDocSaveResponse {
    pub doc: ThreadAgentsDocPayload,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
pub struct ThreadAgentsDocArchiveParams {
    pub workspace_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub folder_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_version: Option<i64>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
pub struct ThreadAgentsDocArchiveResponse {
    pub archived: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective: Option<ThreadAgentsDocResolvedPayload>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
pub struct ThreadAgentsDocResolveForThreadParams {
    pub workspace_id: String,
    pub thread_id: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
pub struct ThreadAgentsDocResolveForThreadResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective: Option<ThreadAgentsDocResolvedPayload>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ThreadAgentsDocChangedNotification {
    pub workspace_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub folder_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub doc: Option<ThreadAgentsDocPayload>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective: Option<ThreadAgentsDocResolvedPayload>,
    pub effective_changed: bool,
}

impl ThreadAgentsDocChangedNotification {
    pub fn event_name(&self) -> &'static str {
        constants::events::THREAD_AGENTS_DOC_CHANGED
    }
}

fn default_save_reason() -> ThreadAgentsDocSaveReason {
    ThreadAgentsDocSaveReason::Autosave
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn constants_include_thread_agents_doc_methods_and_event() {
        assert_eq!(
            constants::methods::THREAD_AGENTS_DOC_GET,
            "thread/agents_doc/get"
        );
        assert_eq!(
            constants::methods::THREAD_AGENTS_DOC_SAVE,
            "thread/agents_doc/save"
        );
        assert_eq!(
            constants::methods::THREAD_AGENTS_DOC_ARCHIVE,
            "thread/agents_doc/archive"
        );
        assert_eq!(
            constants::methods::THREAD_AGENTS_DOC_RESOLVE_FOR_THREAD,
            "thread/agents_doc/resolve_for_thread"
        );
        assert_eq!(
            constants::events::THREAD_AGENTS_DOC_CHANGED,
            "thread/agents_doc/changed"
        );
    }

    #[test]
    fn status_and_save_reason_use_snake_case_json() {
        assert_eq!(
            serde_json::to_value(ThreadAgentsDocStatus::Active).expect("serialize status"),
            json!("active")
        );
        assert_eq!(
            serde_json::to_value(ThreadAgentsDocSaveReason::Autosave)
                .expect("serialize save reason"),
            json!("autosave")
        );
    }

    #[test]
    fn save_params_default_to_autosave() {
        let params: ThreadAgentsDocSaveParams = serde_json::from_value(json!({
            "workspace_id": "ws1",
            "content": "hello"
        }))
        .expect("decode save params");
        assert_eq!(params.save_reason, ThreadAgentsDocSaveReason::Autosave);
    }

    #[test]
    fn schema_documents_include_thread_agents_doc_contracts() {
        let names = crate::protocol_schema_documents()
            .into_iter()
            .map(|document| document.file_name)
            .collect::<Vec<_>>();

        for expected in [
            "thread_agents_doc_payload.json",
            "thread_agents_doc_summary.json",
            "thread_agents_doc_get_params.json",
            "thread_agents_doc_save_params.json",
            "thread_agents_doc_changed_notification.json",
        ] {
            assert!(
                names.contains(&expected),
                "schema list should include {expected}"
            );
        }
    }
}
