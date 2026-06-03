use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::MarkdownDocument;
use crate::thread_agents_doc::ThreadAgentsDocSummary;
use crate::turn::{
    ItemDeltaStream, RecoveryAction, RecoveryJobStatus, RecoveryTrigger, ToolLoopBudgetAction,
    ToolLoopBudgetLimitKind, ToolRetryBudgetUsage, ToolRetryErrorClass, ToolRetryExhaustionKind,
    ToolRetryResolution, Turn, TurnExecutionWindowBlockedNotification,
    TurnExecutionWindowCheckpointedNotification, TurnExecutionWindowContinuedNotification,
    TurnExecutionWindowExhaustedNotification, TurnExecutionWindowStartedNotification, TurnItem,
    TurnItemTimeoutReason, TurnItemType, UserInput,
};

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxMode {
    FullAccess,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadMode {
    Chat,
    Agent,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ThreadOriginKind {
    User,
    TaskRun,
    System,
}

impl ThreadOriginKind {
    pub const fn user() -> Self {
        Self::User
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ThreadSidebarVisibility {
    Visible,
    Hidden,
}

impl ThreadSidebarVisibility {
    pub const fn visible() -> Self {
        Self::Visible
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct ThreadStartParams {
    pub thread_id: String,
    pub workspace_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox: Option<SandboxMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<ThreadMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_kind: Option<ThreadOriginKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sidebar_visibility: Option<ThreadSidebarVisibility>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_nickname: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_role: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct SandboxPolicy {
    pub mode: SandboxMode,
}

impl SandboxPolicy {
    pub fn from_mode(mode: SandboxMode) -> Self {
        Self { mode }
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ThreadStartResponse {
    pub thread: Thread,
    pub sandbox: SandboxPolicy,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
pub struct ThreadTreeParams {
    pub workspace_id: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
pub struct ThreadUpdateParams {
    pub workspace_id: String,
    pub thread_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ThreadUpdateResponse {
    pub thread: Thread,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ThreadTreeResponse {
    pub workspace_id: String,
    #[serde(default)]
    pub threads: Vec<Thread>,
    #[serde(default)]
    pub folders: Vec<ThreadFolder>,
    #[serde(default)]
    pub placements: Vec<ThreadPlacement>,
    #[serde(default)]
    pub agents_docs: Vec<ThreadAgentsDocSummary>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ThreadFolder {
    pub id: String,
    pub workspace_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_folder_id: Option<String>,
    pub name: String,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ThreadPlacement {
    pub thread_id: String,
    pub workspace_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub folder_id: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
pub struct ThreadFolderCreateParams {
    pub workspace_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_folder_id: Option<String>,
    pub name: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ThreadFolderCreateResponse {
    pub folder: ThreadFolder,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
pub struct ThreadFolderDeleteParams {
    pub workspace_id: String,
    pub folder_id: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ThreadFolderDeleteResponse {
    pub deleted: bool,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
pub struct ThreadMoveParams {
    pub workspace_id: String,
    pub thread_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub folder_id: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ThreadMoveResponse {
    pub moved: bool,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
pub struct ThreadFolderMoveParams {
    pub workspace_id: String,
    pub folder_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_folder_id: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ThreadFolderMoveResponse {
    pub moved: bool,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
pub struct ThreadGetParams {
    pub thread_id: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ThreadGetResponse {
    pub thread: Thread,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
pub struct ThreadHistoryParams {
    pub thread_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ThreadHistoryEventPayload {
    #[serde(rename_all = "camelCase")]
    TurnStarted {
        workspace_id: String,
        thread_id: String,
        turn: Turn,
        #[serde(default)]
        input: Vec<UserInput>,
    },
    #[serde(rename_all = "camelCase")]
    ItemStarted {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item: TurnItem,
    },
    #[serde(rename_all = "camelCase")]
    ItemDelta {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        delta: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stream: Option<ItemDeltaStream>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        payload: Option<serde_json::Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        markdown: Option<MarkdownDocument>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        markdown_version: Option<u16>,
    },
    #[serde(rename_all = "camelCase")]
    ItemCompleted {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item: TurnItem,
    },
    #[serde(rename_all = "camelCase")]
    ItemUpdated {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item: TurnItem,
    },
    #[serde(rename_all = "camelCase")]
    ItemTimeoutDetected {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        attempt_number: u32,
        reason: TurnItemTimeoutReason,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        recovery_job_id: Option<String>,
    },
    #[serde(rename_all = "camelCase")]
    ItemRecoveryOpened {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        recovery_job_id: String,
        trigger: RecoveryTrigger,
        action: RecoveryAction,
        attempt_number: u32,
    },
    #[serde(rename_all = "camelCase")]
    ItemRecoveryAttached {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        recovery_job_id: String,
        recovery_item_id: String,
        recovery_item_type: TurnItemType,
        trigger: RecoveryTrigger,
        action: RecoveryAction,
        existing_status: RecoveryJobStatus,
        next_attempt_number: u32,
    },
    #[serde(rename_all = "camelCase")]
    ItemRetryScheduled {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        recovery_job_id: String,
        attempt_number: u32,
        next_run_at_unix: i64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    #[serde(rename_all = "camelCase")]
    ItemRetryAttemptStarted {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        recovery_job_id: String,
        attempt_number: u32,
    },
    #[serde(rename_all = "camelCase")]
    ItemRecoverySucceeded {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        recovery_job_id: String,
        attempt_number: u32,
    },
    #[serde(rename_all = "camelCase")]
    ItemRecoveryExhausted {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        recovery_job_id: String,
        attempt_number: u32,
        status: RecoveryJobStatus,
        error_message: String,
    },
    #[serde(rename_all = "camelCase")]
    ItemToolRetryScheduled {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        tool_retry_episode_id: String,
        tool_name: String,
        attempt_number: u32,
        error_class: ToolRetryErrorClass,
        retry_hint: String,
        budgets: Vec<ToolRetryBudgetUsage>,
        failure_signature_fingerprint: String,
        reason: String,
    },
    #[serde(rename_all = "camelCase")]
    ItemToolRetryResolved {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        tool_retry_episode_id: String,
        tool_name: String,
        attempt_number: u32,
        resolution: ToolRetryResolution,
        budgets: Vec<ToolRetryBudgetUsage>,
        reason: String,
    },
    #[serde(rename_all = "camelCase")]
    ItemToolRetryExhausted {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        tool_retry_episode_id: String,
        tool_name: String,
        attempt_number: u32,
        error_class: ToolRetryErrorClass,
        exhaustion_kind: ToolRetryExhaustionKind,
        budgets: Vec<ToolRetryBudgetUsage>,
        failure_signature_fingerprint: String,
        reason: String,
    },
    #[serde(rename_all = "camelCase")]
    TurnToolLoopBudgetExceeded {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        limit_kind: ToolLoopBudgetLimitKind,
        limit: u32,
        observed: u32,
        action: ToolLoopBudgetAction,
        reason: String,
    },
    #[serde(rename_all = "camelCase")]
    TurnExecutionWindowStarted(TurnExecutionWindowStartedNotification),
    #[serde(rename_all = "camelCase")]
    TurnExecutionWindowExhausted(TurnExecutionWindowExhaustedNotification),
    #[serde(rename_all = "camelCase")]
    TurnExecutionWindowCheckpointed(TurnExecutionWindowCheckpointedNotification),
    #[serde(rename_all = "camelCase")]
    TurnExecutionWindowContinued(TurnExecutionWindowContinuedNotification),
    #[serde(rename_all = "camelCase")]
    TurnExecutionWindowBlocked(TurnExecutionWindowBlockedNotification),
    #[serde(rename_all = "camelCase")]
    TurnCompleted {
        workspace_id: String,
        thread_id: String,
        turn: Turn,
    },
    #[serde(rename_all = "camelCase")]
    TurnFailed {
        workspace_id: String,
        thread_id: String,
        turn: Turn,
    },
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ThreadHistoryEvent {
    pub turn_id: String,
    pub sequence: i64,
    pub created_at: i64,
    pub payload: ThreadHistoryEventPayload,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ThreadHistoryResponse {
    pub workspace_id: String,
    pub thread_id: String,
    #[serde(default)]
    pub events: Vec<ThreadHistoryEvent>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ThreadUnsubscribeParams {
    pub thread_id: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ThreadUnsubscribeResponse {
    pub status: ThreadUnsubscribeStatus,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ThreadUnsubscribeStatus {
    NotLoaded,
    NotSubscribed,
    Unsubscribed,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct Thread {
    pub workspace_id: String,
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub preview: String,
    pub mode: ThreadMode,
    pub model: String,
    pub model_provider: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub status: ThreadStatus,
    #[serde(default = "ThreadOriginKind::user")]
    pub origin_kind: ThreadOriginKind,
    #[serde(default = "ThreadSidebarVisibility::visible")]
    pub sidebar_visibility: ThreadSidebarVisibility,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_nickname: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_role: Option<String>,
    pub turns: Vec<Turn>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadStatus {
    Active,
    Idle,
    Closed,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ThreadStartedNotification {
    pub thread: Thread,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ThreadClosedNotification {
    pub workspace_id: String,
    pub thread_id: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ThreadUpdatedNotification {
    pub thread: Thread,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ThreadTreeChangedNotification {
    pub workspace_id: String,
}

#[cfg(test)]
mod tests {
    use super::{
        Thread, ThreadClosedNotification, ThreadMode, ThreadOriginKind, ThreadSidebarVisibility,
        ThreadStartParams, ThreadStatus, ThreadTreeResponse, ThreadUnsubscribeParams,
        ThreadUnsubscribeStatus,
    };
    use crate::{ThreadAgentsDocStatus, ThreadAgentsDocSummary};
    use serde_json::json;

    #[test]
    fn thread_start_params_preserve_explicit_null_mode() {
        let params: ThreadStartParams = serde_json::from_value(json!({
            "thread_id": "thr_123",
            "workspace_id": "ws_123",
            "mode": null
        }))
        .expect("params should decode");

        assert_eq!(params.mode, None);

        let encoded = serde_json::to_value(params).expect("params should encode");
        assert_eq!(encoded.get("mode"), None);
    }

    #[test]
    fn thread_mode_serializes_as_plain_enum_value() {
        let encoded = serde_json::to_value(ThreadMode::Chat).expect("mode should encode");
        assert_eq!(encoded, json!("Chat"));

        let encoded = serde_json::to_value(ThreadMode::Agent).expect("mode should encode");
        assert_eq!(encoded, json!("Agent"));
    }

    #[test]
    fn thread_start_params_round_trip_workspace_id() {
        let params: ThreadStartParams = serde_json::from_value(json!({
            "thread_id": "thr_123",
            "workspace_id": "ws_123"
        }))
        .expect("params should decode");
        assert_eq!(params.workspace_id, "ws_123");

        let encoded = serde_json::to_value(params).expect("params should encode");
        assert_eq!(
            encoded,
            json!({"thread_id": "thr_123", "workspace_id": "ws_123"})
        );
    }

    #[test]
    fn thread_tree_response_defaults_agents_docs_and_omits_content_in_summaries() {
        let decoded: ThreadTreeResponse = serde_json::from_value(json!({
            "workspace_id": "ws_123",
            "threads": [],
            "folders": [],
            "placements": []
        }))
        .expect("thread tree response should decode without agents_docs");
        assert!(decoded.agents_docs.is_empty());

        let encoded = serde_json::to_value(ThreadTreeResponse {
            workspace_id: "ws_123".to_owned(),
            threads: Vec::new(),
            folders: Vec::new(),
            placements: Vec::new(),
            agents_docs: vec![ThreadAgentsDocSummary {
                id: "doc_123".to_owned(),
                workspace_id: "ws_123".to_owned(),
                folder_id: None,
                status: ThreadAgentsDocStatus::Active,
                content_sha256: "hash".to_owned(),
                version: 2,
                char_count: 12,
                updated_at: 10,
            }],
        })
        .expect("thread tree response should encode");
        let summary = encoded
            .get("agents_docs")
            .and_then(serde_json::Value::as_array)
            .and_then(|items| items.first())
            .expect("summary should be present");
        assert!(summary.get("content").is_none());
        assert_eq!(summary.get("char_count"), Some(&json!(12)));
    }

    #[test]
    fn thread_start_params_require_workspace_id() {
        let error = serde_json::from_value::<ThreadStartParams>(json!({
            "thread_id": "thr_123"
        }))
        .expect_err("workspace_id is required");
        assert!(error.to_string().contains("workspace_id"));
    }

    #[test]
    fn thread_start_params_round_trip_thread_id() {
        let params: ThreadStartParams = serde_json::from_value(json!({
            "workspace_id": "ws_123",
            "thread_id": "thr_123"
        }))
        .expect("params should decode");
        assert_eq!(params.thread_id, "thr_123");

        let encoded = serde_json::to_value(params).expect("params should encode");
        assert_eq!(
            encoded,
            json!({"thread_id": "thr_123", "workspace_id": "ws_123"})
        );
    }

    #[test]
    fn thread_start_params_require_thread_id() {
        let error = serde_json::from_value::<ThreadStartParams>(json!({
            "workspace_id": "ws_123"
        }))
        .expect_err("thread_id is required");
        assert!(error.to_string().contains("thread_id"));
    }

    #[test]
    fn thread_origin_and_visibility_default_for_decoding() {
        let thread: Thread = serde_json::from_value(json!({
            "workspace_id": "ws_123",
            "id": "thr_123",
            "preview": "",
            "mode": "Chat",
            "model": "gpt-5.4",
            "model_provider": "openai",
            "created_at": 1,
            "updated_at": 1,
            "status": "Idle",
            "turns": []
        }))
        .expect("thread should decode with origin defaults");

        assert_eq!(thread.origin_kind, ThreadOriginKind::User);
        assert_eq!(thread.sidebar_visibility, ThreadSidebarVisibility::Visible);
        assert_eq!(thread.status, ThreadStatus::Idle);
    }

    #[test]
    fn thread_unsubscribe_params_use_thread_id_camel_case() {
        let params: ThreadUnsubscribeParams = serde_json::from_value(json!({
            "threadId": "thr_123"
        }))
        .expect("params should decode");
        assert_eq!(params.thread_id, "thr_123");

        let encoded = serde_json::to_value(params).expect("params should encode");
        assert_eq!(encoded, json!({"threadId": "thr_123"}));
    }

    #[test]
    fn thread_unsubscribe_status_uses_camel_case() {
        let encoded =
            serde_json::to_value(ThreadUnsubscribeStatus::NotSubscribed).expect("status encode");
        assert_eq!(encoded, json!("notSubscribed"));
    }

    #[test]
    fn thread_closed_notification_uses_thread_id_camel_case() {
        let notification = ThreadClosedNotification {
            workspace_id: "ws_123".to_owned(),
            thread_id: "thr_123".to_owned(),
        };
        let encoded = serde_json::to_value(notification).expect("notification should encode");
        assert_eq!(
            encoded,
            json!({"workspaceId": "ws_123", "threadId": "thr_123"})
        );
    }
}
