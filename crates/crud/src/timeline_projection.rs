use pioneer_entity::{turn, turn_item};
use pioneer_protocol::{
    AgentMessagePhase, SystemEventLevel, TaskAttachmentMode, ToolCallStatus, TurnItem,
};
use serde_json::Value as JsonValue;

use crate::repositories::thread_timeline_projection::{
    WORK_VISIBILITY_HIDDEN, WORK_VISIBILITY_VISIBLE,
};

pub const WORK_ITEM_STATUS_RUNNING: &str = "running";
pub const WORK_ITEM_STATUS_COMPLETED: &str = "completed";
pub const WORK_ITEM_STATUS_FAILED: &str = "failed";
pub const WORK_ITEM_STATUS_CANCELLED: &str = "cancelled";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionVisibility {
    Visible,
    Hidden,
}

impl ProjectionVisibility {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Visible => WORK_VISIBILITY_VISIBLE,
            Self::Hidden => WORK_VISIBILITY_HIDDEN,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionPlacement {
    TopLevelUserMessage,
    TopLevelDetachedTaskRun,
    TurnWork,
    TopLevelAssistantMessage,
    Hidden,
}

impl ProjectionPlacement {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TopLevelUserMessage => "top_level_user_message",
            Self::TopLevelDetachedTaskRun => "top_level_detached_task_run",
            Self::TurnWork => "turn_work",
            Self::TopLevelAssistantMessage => "top_level_assistant_message",
            Self::Hidden => "hidden",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkItemClassification {
    UserMessage,
    FinalAssistantMessage,
    AgentCommentary,
    Reasoning,
    Task,
    CommandExecution,
    FileChange,
    WebSearch,
    WebFetch,
    Download,
    DynamicToolCall,
    SystemRecovery,
    SystemExecutionWindow,
    SystemError,
    SystemWarning,
    AgentContextCompaction,
    AgentReview,
    InternalTokenUsage,
    InternalThreadStatus,
    InternalDiffUpdate,
    InternalPlanUpdate,
    InternalRuntimeEvent,
    InternalAgentRuntimeItem,
    UnknownSystemEvent,
    InvalidPayload,
}

impl WorkItemClassification {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UserMessage => "user_message",
            Self::FinalAssistantMessage => "final_assistant_message",
            Self::AgentCommentary => "agent_commentary",
            Self::Reasoning => "reasoning",
            Self::Task => "task",
            Self::CommandExecution => "command_execution",
            Self::FileChange => "file_change",
            Self::WebSearch => "web_search",
            Self::WebFetch => "web_fetch",
            Self::Download => "download",
            Self::DynamicToolCall => "dynamic_tool_call",
            Self::SystemRecovery => "system_recovery",
            Self::SystemExecutionWindow => "system_execution_window",
            Self::SystemError => "system_error",
            Self::SystemWarning => "system_warning",
            Self::AgentContextCompaction => "agent_context_compaction",
            Self::AgentReview => "agent_review",
            Self::InternalTokenUsage => "internal_token_usage",
            Self::InternalThreadStatus => "internal_thread_status",
            Self::InternalDiffUpdate => "internal_diff_update",
            Self::InternalPlanUpdate => "internal_plan_update",
            Self::InternalRuntimeEvent => "internal_runtime_event",
            Self::InternalAgentRuntimeItem => "internal_agent_runtime_item",
            Self::UnknownSystemEvent => "unknown_system_event",
            Self::InvalidPayload => "invalid_payload",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnItemProjectionClassification {
    pub item_id: String,
    pub item_type: String,
    pub visibility: ProjectionVisibility,
    pub placement: ProjectionPlacement,
    pub classification: WorkItemClassification,
    pub status: &'static str,
    pub audit: bool,
    pub audit_reason: Option<String>,
}

impl TurnItemProjectionClassification {
    pub fn visibility_str(&self) -> &'static str {
        self.visibility.as_str()
    }

    pub fn placement_str(&self) -> &'static str {
        self.placement.as_str()
    }

    pub fn classification_str(&self) -> &'static str {
        self.classification.as_str()
    }
}

pub fn classify_turn_item_row(row: &turn_item::Model) -> TurnItemProjectionClassification {
    let item_id = row.item_id.clone();
    let item_type = row.item_type.clone();
    let db_status = db_status_to_work_status(row.status.as_deref());

    match normalized_item_type(row.item_type.as_str()).as_str() {
        "usermessage" => TurnItemProjectionClassification {
            item_id,
            item_type,
            visibility: ProjectionVisibility::Visible,
            placement: ProjectionPlacement::TopLevelUserMessage,
            classification: WorkItemClassification::UserMessage,
            status: db_status,
            audit: false,
            audit_reason: None,
        },
        "agentmessage" => {
            let phase = match parse_turn_item_payload(row) {
                Ok(payload) => payload
                    .get("phase")
                    .and_then(JsonValue::as_str)
                    .map(normalized_wire_value),
                Err(error) => return invalid_payload(row, error),
            };
            if phase.as_deref().is_none_or(|phase| phase == "finalanswer") {
                TurnItemProjectionClassification {
                    item_id,
                    item_type,
                    visibility: ProjectionVisibility::Visible,
                    placement: ProjectionPlacement::TopLevelAssistantMessage,
                    classification: WorkItemClassification::FinalAssistantMessage,
                    status: db_status,
                    audit: false,
                    audit_reason: None,
                }
            } else {
                visible_work(
                    item_id,
                    item_type,
                    WorkItemClassification::AgentCommentary,
                    db_status,
                )
            }
        }
        "reasoning" => visible_work(
            item_id,
            item_type,
            WorkItemClassification::Reasoning,
            db_status,
        ),
        "task" => visible_work(item_id, item_type, WorkItemClassification::Task, db_status),
        "commandexecution" => visible_work(
            item_id,
            item_type,
            WorkItemClassification::CommandExecution,
            tool_status_from_payload(row).unwrap_or(db_status),
        ),
        "filechange" => visible_work(
            item_id,
            item_type,
            WorkItemClassification::FileChange,
            tool_status_from_payload(row).unwrap_or(db_status),
        ),
        "websearch" => visible_work(
            item_id,
            item_type,
            WorkItemClassification::WebSearch,
            tool_status_from_payload(row).unwrap_or(db_status),
        ),
        "webfetch" => visible_work(
            item_id,
            item_type,
            WorkItemClassification::WebFetch,
            tool_status_from_payload(row).unwrap_or(db_status),
        ),
        "download" => visible_work(
            item_id,
            item_type,
            WorkItemClassification::Download,
            tool_status_from_payload(row).unwrap_or(db_status),
        ),
        "dynamictoolcall" => visible_work(
            item_id,
            item_type,
            WorkItemClassification::DynamicToolCall,
            tool_status_from_payload(row).unwrap_or(db_status),
        ),
        "systemevent" => {
            let payload = match parse_turn_item_payload(row) {
                Ok(payload) => payload,
                Err(error) => return invalid_payload(row, error),
            };
            let level = payload
                .get("level")
                .and_then(JsonValue::as_str)
                .and_then(system_event_level_from_wire);
            let Some(level) = level else {
                return invalid_payload(row, "missing or invalid system event level");
            };
            let Some(message) = payload.get("message").and_then(JsonValue::as_str) else {
                return invalid_payload(row, "missing system event message");
            };
            let status = system_event_work_status(level);
            classify_system_event(
                item_id,
                item_type,
                level,
                message,
                payload.get("code").and_then(JsonValue::as_str),
                payload.get("details"),
                status,
            )
        }
        _ => TurnItemProjectionClassification {
            item_id,
            item_type,
            visibility: ProjectionVisibility::Hidden,
            placement: ProjectionPlacement::Hidden,
            classification: WorkItemClassification::InvalidPayload,
            status: db_status,
            audit: true,
            audit_reason: Some(format!("unknown turn item type `{}`", row.item_type)),
        },
    }
}

pub fn classify_turn_item_row_for_turn(
    row: &turn_item::Model,
    turn: &turn::Model,
) -> TurnItemProjectionClassification {
    let mut classification = classify_turn_item_row(row);
    if turn.turn_kind == "task_run"
        && classification.classification == WorkItemClassification::Task
        && task_attachment_from_row(row) == Some(TaskAttachmentMode::Detached)
    {
        classification.placement = ProjectionPlacement::TopLevelDetachedTaskRun;
    }
    classification
}

fn task_attachment_from_row(row: &turn_item::Model) -> Option<TaskAttachmentMode> {
    let TurnItem::Task { item } = serde_json::from_str::<TurnItem>(row.payload.as_str()).ok()?
    else {
        return None;
    };
    Some(item.attachment)
}

fn parse_turn_item_payload(row: &turn_item::Model) -> std::result::Result<JsonValue, String> {
    serde_json::from_str::<JsonValue>(row.payload.as_str()).map_err(|error| error.to_string())
}

fn invalid_payload(
    row: &turn_item::Model,
    reason: impl Into<String>,
) -> TurnItemProjectionClassification {
    TurnItemProjectionClassification {
        item_id: row.item_id.clone(),
        item_type: row.item_type.clone(),
        visibility: ProjectionVisibility::Hidden,
        placement: ProjectionPlacement::Hidden,
        classification: WorkItemClassification::InvalidPayload,
        status: db_status_to_work_status(row.status.as_deref()),
        audit: true,
        audit_reason: Some(format!(
            "failed to classify turn item payload: {}",
            reason.into()
        )),
    }
}

fn tool_status_from_payload(row: &turn_item::Model) -> Option<&'static str> {
    let payload = parse_turn_item_payload(row).ok()?;
    let status = payload.get("status").and_then(JsonValue::as_str)?;
    match normalized_wire_value(status).as_str() {
        "inprogress" => Some(WORK_ITEM_STATUS_RUNNING),
        "completed" => Some(WORK_ITEM_STATUS_COMPLETED),
        "failed" => Some(WORK_ITEM_STATUS_FAILED),
        _ => None,
    }
}

fn system_event_level_from_wire(value: &str) -> Option<SystemEventLevel> {
    match normalized_wire_value(value).as_str() {
        "info" => Some(SystemEventLevel::Info),
        "warning" => Some(SystemEventLevel::Warning),
        "error" => Some(SystemEventLevel::Error),
        _ => None,
    }
}

fn system_event_work_status(level: SystemEventLevel) -> &'static str {
    match level {
        SystemEventLevel::Info | SystemEventLevel::Warning => WORK_ITEM_STATUS_COMPLETED,
        SystemEventLevel::Error => WORK_ITEM_STATUS_FAILED,
    }
}

fn normalized_item_type(value: &str) -> String {
    normalized_wire_value(value)
}

fn normalized_wire_value(value: &str) -> String {
    value
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_lowercase()
}

pub fn classify_turn_item_with_db_status(
    item: &TurnItem,
    db_status: Option<&str>,
) -> TurnItemProjectionClassification {
    let item_id = item.item_id().to_owned();
    let item_type = turn_item_type_label(item).to_owned();
    let status = turn_item_work_status(item).unwrap_or_else(|| db_status_to_work_status(db_status));

    match item {
        TurnItem::UserMessage { .. } => TurnItemProjectionClassification {
            item_id,
            item_type,
            visibility: ProjectionVisibility::Visible,
            placement: ProjectionPlacement::TopLevelUserMessage,
            classification: WorkItemClassification::UserMessage,
            status,
            audit: false,
            audit_reason: None,
        },
        TurnItem::AgentMessage { phase, .. } => {
            if matches!(phase, AgentMessagePhase::FinalAnswer) {
                TurnItemProjectionClassification {
                    item_id,
                    item_type,
                    visibility: ProjectionVisibility::Visible,
                    placement: ProjectionPlacement::TopLevelAssistantMessage,
                    classification: WorkItemClassification::FinalAssistantMessage,
                    status,
                    audit: false,
                    audit_reason: None,
                }
            } else {
                TurnItemProjectionClassification {
                    item_id,
                    item_type,
                    visibility: ProjectionVisibility::Visible,
                    placement: ProjectionPlacement::TurnWork,
                    classification: WorkItemClassification::AgentCommentary,
                    status,
                    audit: false,
                    audit_reason: None,
                }
            }
        }
        TurnItem::Reasoning { .. } => visible_work(
            item_id,
            item_type,
            WorkItemClassification::Reasoning,
            status,
        ),
        TurnItem::Task { .. } => {
            visible_work(item_id, item_type, WorkItemClassification::Task, status)
        }
        TurnItem::CommandExecution { .. } => visible_work(
            item_id,
            item_type,
            WorkItemClassification::CommandExecution,
            status,
        ),
        TurnItem::FileChange { .. } => visible_work(
            item_id,
            item_type,
            WorkItemClassification::FileChange,
            status,
        ),
        TurnItem::WebSearch { .. } => visible_work(
            item_id,
            item_type,
            WorkItemClassification::WebSearch,
            status,
        ),
        TurnItem::WebFetch { .. } => {
            visible_work(item_id, item_type, WorkItemClassification::WebFetch, status)
        }
        TurnItem::Download { .. } => {
            visible_work(item_id, item_type, WorkItemClassification::Download, status)
        }
        TurnItem::DynamicToolCall { .. } => visible_work(
            item_id,
            item_type,
            WorkItemClassification::DynamicToolCall,
            status,
        ),
        TurnItem::SystemEvent {
            level,
            message,
            code,
            details,
            ..
        } => classify_system_event(
            item_id,
            item_type,
            *level,
            message,
            code.as_deref(),
            details.as_ref(),
            status,
        ),
    }
}

fn visible_work(
    item_id: String,
    item_type: String,
    classification: WorkItemClassification,
    status: &'static str,
) -> TurnItemProjectionClassification {
    TurnItemProjectionClassification {
        item_id,
        item_type,
        visibility: ProjectionVisibility::Visible,
        placement: ProjectionPlacement::TurnWork,
        classification,
        status,
        audit: false,
        audit_reason: None,
    }
}

fn hidden_work(
    item_id: String,
    item_type: String,
    classification: WorkItemClassification,
    status: &'static str,
    audit_reason: impl Into<String>,
) -> TurnItemProjectionClassification {
    TurnItemProjectionClassification {
        item_id,
        item_type,
        visibility: ProjectionVisibility::Hidden,
        placement: ProjectionPlacement::Hidden,
        classification,
        status,
        audit: true,
        audit_reason: Some(audit_reason.into()),
    }
}

fn classify_system_event(
    item_id: String,
    item_type: String,
    level: SystemEventLevel,
    message: &str,
    code: Option<&str>,
    details: Option<&JsonValue>,
    status: &'static str,
) -> TurnItemProjectionClassification {
    if matches!(level, SystemEventLevel::Error) {
        return visible_work(
            item_id,
            item_type,
            WorkItemClassification::SystemError,
            status,
        );
    }

    if matches!(level, SystemEventLevel::Warning) {
        return visible_work(
            item_id,
            item_type,
            WorkItemClassification::SystemWarning,
            status,
        );
    }

    match code {
        Some("agent_thread_status_changed") => hidden_work(
            item_id,
            item_type,
            WorkItemClassification::InternalThreadStatus,
            status,
            "internal thread status update",
        ),
        Some("agent_diff_updated") => hidden_work(
            item_id,
            item_type,
            WorkItemClassification::InternalDiffUpdate,
            status,
            "internal turn diff update",
        ),
        Some("agent_plan_updated") => hidden_work(
            item_id,
            item_type,
            WorkItemClassification::InternalPlanUpdate,
            status,
            "internal plan update",
        ),
        Some("agent_runtime_event") => {
            classify_runtime_event(item_id, item_type, message, details, status)
        }
        Some("agent_runtime_item") => hidden_work(
            item_id,
            item_type,
            WorkItemClassification::InternalAgentRuntimeItem,
            status,
            "unmapped info-level runtime item",
        ),
        Some("agent_context_compaction") => visible_work(
            item_id,
            item_type,
            WorkItemClassification::AgentContextCompaction,
            status,
        ),
        Some("agent_review") => visible_work(
            item_id,
            item_type,
            WorkItemClassification::AgentReview,
            status,
        ),
        Some(code) if is_recovery_system_code(code) => visible_work(
            item_id,
            item_type,
            WorkItemClassification::SystemRecovery,
            status,
        ),
        Some(code) if is_execution_window_system_code(code) => visible_work(
            item_id,
            item_type,
            WorkItemClassification::SystemExecutionWindow,
            status,
        ),
        _ if message.starts_with("Thread status changed:") => hidden_work(
            item_id,
            item_type,
            WorkItemClassification::InternalThreadStatus,
            status,
            "internal thread status update",
        ),
        _ if message == "Diff updated" => hidden_work(
            item_id,
            item_type,
            WorkItemClassification::InternalDiffUpdate,
            status,
            "internal turn diff update",
        ),
        _ if message == "Plan updated" => hidden_work(
            item_id,
            item_type,
            WorkItemClassification::InternalPlanUpdate,
            status,
            "internal plan update",
        ),
        _ if message.starts_with("Runtime event: ") => hidden_work(
            item_id,
            item_type,
            WorkItemClassification::InternalRuntimeEvent,
            status,
            "unmapped info-level runtime event",
        ),
        _ => hidden_work(
            item_id,
            item_type,
            WorkItemClassification::UnknownSystemEvent,
            status,
            "unknown info-level system event",
        ),
    }
}

fn classify_runtime_event(
    item_id: String,
    item_type: String,
    message: &str,
    details: Option<&JsonValue>,
    status: &'static str,
) -> TurnItemProjectionClassification {
    let native_method = details
        .and_then(|details| details.get("nativeMethod"))
        .and_then(JsonValue::as_str)
        .or_else(|| message.strip_prefix("Runtime event: "));

    match native_method {
        Some("thread/tokenUsage/updated") => hidden_work(
            item_id,
            item_type,
            WorkItemClassification::InternalTokenUsage,
            status,
            "internal token usage update",
        ),
        Some(
            "fuzzyFileSearch/sessionUpdated"
            | "fuzzyFileSearch/sessionCompleted"
            | "windowsSandbox/setupCompleted",
        ) => hidden_work(
            item_id,
            item_type,
            WorkItemClassification::InternalRuntimeEvent,
            status,
            "internal runtime lifecycle event",
        ),
        Some(other) => hidden_work(
            item_id,
            item_type,
            WorkItemClassification::InternalRuntimeEvent,
            status,
            format!("unmapped info-level runtime event `{other}`"),
        ),
        None => hidden_work(
            item_id,
            item_type,
            WorkItemClassification::InternalRuntimeEvent,
            status,
            "runtime event without native method",
        ),
    }
}

fn is_recovery_system_code(code: &str) -> bool {
    matches!(
        code,
        "turn_start_rejected"
            | "turn_blocked_resumable"
            | "item_timeout_detected"
            | "item_recovery_opened"
            | "item_recovery_attached"
            | "item_retry_scheduled"
            | "item_retry_attempt_started"
            | "item_recovery_succeeded"
            | "item_recovery_exhausted"
            | "item_tool_retry_scheduled"
            | "item_tool_retry_resolved"
            | "item_tool_retry_exhausted"
            | "turn_tool_loop_budget_exceeded"
    )
}

fn is_execution_window_system_code(code: &str) -> bool {
    matches!(
        code,
        "turn_execution_window_exhausted"
            | "turn_execution_window_continued"
            | "turn_execution_window_blocked"
    )
}

fn turn_item_work_status(item: &TurnItem) -> Option<&'static str> {
    let status = match item {
        TurnItem::CommandExecution { status, .. }
        | TurnItem::FileChange { status, .. }
        | TurnItem::WebSearch { status, .. }
        | TurnItem::WebFetch { status, .. }
        | TurnItem::Download { status, .. }
        | TurnItem::DynamicToolCall { status, .. } => match status {
            ToolCallStatus::InProgress => WORK_ITEM_STATUS_RUNNING,
            ToolCallStatus::Completed => WORK_ITEM_STATUS_COMPLETED,
            ToolCallStatus::Failed => WORK_ITEM_STATUS_FAILED,
        },
        TurnItem::SystemEvent { level, .. } => match level {
            SystemEventLevel::Info | SystemEventLevel::Warning => WORK_ITEM_STATUS_COMPLETED,
            SystemEventLevel::Error => WORK_ITEM_STATUS_FAILED,
        },
        _ => return None,
    };
    Some(status)
}

fn db_status_to_work_status(db_status: Option<&str>) -> &'static str {
    match db_status {
        Some("in_progress") | Some("running") => WORK_ITEM_STATUS_RUNNING,
        Some("failed") | Some("timed_out") => WORK_ITEM_STATUS_FAILED,
        Some("cancelled") | Some("canceled") => WORK_ITEM_STATUS_CANCELLED,
        Some("completed") | None => WORK_ITEM_STATUS_COMPLETED,
        Some(_) => WORK_ITEM_STATUS_COMPLETED,
    }
}

fn turn_item_type_label(item: &TurnItem) -> &'static str {
    match item {
        TurnItem::UserMessage { .. } => "user_message",
        TurnItem::AgentMessage { .. } => "agent_message",
        TurnItem::Reasoning { .. } => "reasoning",
        TurnItem::SystemEvent { .. } => "system_event",
        TurnItem::Task { .. } => "task",
        TurnItem::CommandExecution { .. } => "command_execution",
        TurnItem::FileChange { .. } => "file_change",
        TurnItem::WebSearch { .. } => "web_search",
        TurnItem::WebFetch { .. } => "web_fetch",
        TurnItem::Download { .. } => "download",
        TurnItem::DynamicToolCall { .. } => "dynamic_tool_call",
    }
}

#[cfg(test)]
mod tests {
    use pioneer_entity::{turn, turn_item};
    use pioneer_protocol::{
        AgentMessagePhase, SystemEventLevel, TaskAttachmentMode, TaskExecutorKind, TaskStatus,
        TaskTriggerKind, TaskTurnItem, TurnItem,
    };
    use serde_json::json;

    use super::*;

    fn info_system(code: Option<&str>, message: &str, details: Option<JsonValue>) -> TurnItem {
        TurnItem::SystemEvent {
            id: "item_system".to_owned(),
            level: SystemEventLevel::Info,
            message: message.to_owned(),
            code: code.map(str::to_owned),
            details,
        }
    }

    fn turn_item_row(item_type: &str, status: Option<&str>, payload: &str) -> turn_item::Model {
        let now = chrono::Utc::now().into();
        turn_item::Model {
            id: "row_1".to_owned(),
            turn_id: "turn_1".to_owned(),
            item_id: "item_1".to_owned(),
            item_type: item_type.to_owned(),
            status: status.map(str::to_owned),
            payload: payload.to_owned(),
            active_attempt_number: 0,
            active_attempt_status: None,
            active_attempt_id: None,
            last_heartbeat_at: None,
            lease_expires_at: None,
            created_at: now,
            updated_at: now,
        }
    }

    fn turn_row(turn_kind: &str) -> turn::Model {
        let now = chrono::Utc::now().into();
        turn::Model {
            id: "turn_1".to_owned(),
            thread_id: "thread_1".to_owned(),
            initiated_by_actor_id: None,
            initiated_by_actor_kind: None,
            status: "in_progress".to_owned(),
            error: None,
            prompt_manifest_json: "{}".to_owned(),
            prompt_compiler_version: None,
            prompt_profile: None,
            prompt_fingerprint_stable: None,
            prompt_fingerprint_dynamic: None,
            prompt_fingerprint_full: None,
            created_at: now,
            updated_at: now,
            turn_kind: turn_kind.to_owned(),
            origin: "detached_task".to_owned(),
            reasoning_effort: None,
            permission_profile_mode: None,
            permission_profile_source: None,
            permission_profile_snapshot_json: None,
            execution_security_snapshot_version: None,
            execution_security_snapshot_json: None,
            execution_authorization_context_json: None,
            send_mode: None,
            author_display_name_snapshot: None,
            author_nickname_snapshot: None,
            author_avatar_revision_snapshot: None,
            author_agent_snapshot_json: None,
            reply_to_turn_id: None,
            mentions_json: "[]".to_owned(),
            message_revision: 0,
            message_deleted_at: None,
            message_deleted_by_actor_id: None,
            message_deleted_by_actor_kind: None,
        }
    }

    fn task_item(attachment: TaskAttachmentMode) -> TurnItem {
        TurnItem::Task {
            item: TaskTurnItem {
                id: "item_1".to_owned(),
                task_id: "task_1".to_owned(),
                created_by_turn_id: None,
                run_id: Some("run_1".to_owned()),
                parent_task_id: None,
                root_task_id: None,
                title: "Background analysis".to_owned(),
                status: TaskStatus::Running,
                attachment,
                trigger_kind: TaskTriggerKind::Immediate,
                executor_kind: TaskExecutorKind::Agent,
                child_thread_id: Some("child_1".to_owned()),
                child_turn_id: Some("child_turn_1".to_owned()),
                agent_role: None,
                depth: 0,
                max_depth: 3,
                next_fire_at: None,
                progress_preview: Some("Collecting sources".to_owned()),
                result_preview: None,
                error_preview: None,
                started_at: Some(1),
                created_at: 1,
                updated_at: 2,
            },
        }
    }

    #[test]
    fn row_classification_uses_canonical_columns_for_user_message() {
        let row = turn_item_row("user_message", Some("completed"), "not a turn item json");
        let classified = classify_turn_item_row(&row);

        assert_eq!(
            classified.placement,
            ProjectionPlacement::TopLevelUserMessage
        );
        assert_eq!(
            classified.classification,
            WorkItemClassification::UserMessage
        );
        assert!(!classified.audit);
    }

    #[test]
    fn row_classification_uses_lightweight_payload_fields_for_work_items() {
        let row = turn_item_row(
            "agent_message",
            Some("completed"),
            r#"{"type":"agentMessage","id":"commentary","text":"checking","phase":"commentary"}"#,
        );
        let classified = classify_turn_item_row(&row);
        assert_eq!(classified.placement, ProjectionPlacement::TurnWork);
        assert_eq!(
            classified.classification,
            WorkItemClassification::AgentCommentary
        );

        let row = turn_item_row(
            "command_execution",
            Some("completed"),
            r#"{"type":"commandExecution","id":"cmd","status":"failed"}"#,
        );
        let classified = classify_turn_item_row(&row);
        assert_eq!(
            classified.classification,
            WorkItemClassification::CommandExecution
        );
        assert_eq!(classified.status, WORK_ITEM_STATUS_FAILED);

        let row = turn_item_row(
            "system_event",
            Some("completed"),
            r#"{"type":"systemEvent","id":"sys","level":"info","message":"Runtime event: thread/tokenUsage/updated","code":"agent_runtime_event","details":{"nativeMethod":"thread/tokenUsage/updated"}}"#,
        );
        let classified = classify_turn_item_row(&row);
        assert_eq!(classified.visibility, ProjectionVisibility::Hidden);
        assert_eq!(
            classified.classification,
            WorkItemClassification::InternalTokenUsage
        );
    }

    #[test]
    fn only_detached_task_run_anchors_project_as_top_level_cards() {
        let detached_payload =
            serde_json::to_string(&task_item(TaskAttachmentMode::Detached)).unwrap();
        let detached = turn_item_row("task", Some("completed"), detached_payload.as_str());
        assert_eq!(
            classify_turn_item_row_for_turn(&detached, &turn_row("task_run")).placement,
            ProjectionPlacement::TopLevelDetachedTaskRun
        );

        let attached_payload =
            serde_json::to_string(&task_item(TaskAttachmentMode::Attached)).unwrap();
        let attached = turn_item_row("task", Some("completed"), attached_payload.as_str());
        assert_eq!(
            classify_turn_item_row_for_turn(&attached, &turn_row("task_run")).placement,
            ProjectionPlacement::TurnWork
        );
        assert_eq!(
            classify_turn_item_row_for_turn(&detached, &turn_row("conversation")).placement,
            ProjectionPlacement::TurnWork
        );
    }

    #[test]
    fn hides_known_internal_runtime_events() {
        let item = info_system(
            Some("agent_runtime_event"),
            "Runtime event: thread/tokenUsage/updated",
            Some(json!({ "nativeMethod": "thread/tokenUsage/updated" })),
        );
        let classified = classify_turn_item_with_db_status(&item, Some("completed"));
        assert_eq!(classified.visibility, ProjectionVisibility::Hidden);
        assert_eq!(
            classified.classification,
            WorkItemClassification::InternalTokenUsage
        );

        let item = info_system(
            Some("agent_thread_status_changed"),
            "Thread status changed: changed",
            Some(json!({ "status": "changed" })),
        );
        let classified = classify_turn_item_with_db_status(&item, Some("completed"));
        assert_eq!(classified.visibility, ProjectionVisibility::Hidden);
        assert_eq!(
            classified.classification,
            WorkItemClassification::InternalThreadStatus
        );

        let item = info_system(Some("agent_diff_updated"), "Diff updated", None);
        let classified = classify_turn_item_with_db_status(&item, Some("completed"));
        assert_eq!(classified.visibility, ProjectionVisibility::Hidden);
        assert_eq!(
            classified.classification,
            WorkItemClassification::InternalDiffUpdate
        );
    }

    #[test]
    fn keeps_errors_and_recovery_system_events_visible() {
        let item = TurnItem::SystemEvent {
            id: "item_error".to_owned(),
            level: SystemEventLevel::Error,
            message: "tool failed".to_owned(),
            code: Some("agent_runtime_item".to_owned()),
            details: None,
        };
        let classified = classify_turn_item_with_db_status(&item, Some("completed"));
        assert_eq!(classified.visibility, ProjectionVisibility::Visible);
        assert_eq!(
            classified.classification,
            WorkItemClassification::SystemError
        );

        let item = info_system(
            Some("item_recovery_exhausted"),
            "Recovery failed",
            Some(json!({ "item_type": "web_fetch" })),
        );
        let classified = classify_turn_item_with_db_status(&item, Some("completed"));
        assert_eq!(classified.visibility, ProjectionVisibility::Visible);
        assert_eq!(
            classified.classification,
            WorkItemClassification::SystemRecovery
        );
    }

    #[test]
    fn splits_final_answer_from_commentary() {
        let final_item = TurnItem::AgentMessage {
            id: "final".to_owned(),
            text: "done".to_owned(),
            phase: AgentMessagePhase::FinalAnswer,
            markdown: None,
            markdown_version: None,
        };
        let classified = classify_turn_item_with_db_status(&final_item, Some("completed"));
        assert_eq!(
            classified.placement,
            ProjectionPlacement::TopLevelAssistantMessage
        );

        let commentary_item = TurnItem::AgentMessage {
            id: "commentary".to_owned(),
            text: "checking".to_owned(),
            phase: AgentMessagePhase::Commentary,
            markdown: None,
            markdown_version: None,
        };
        let classified = classify_turn_item_with_db_status(&commentary_item, Some("completed"));
        assert_eq!(classified.placement, ProjectionPlacement::TurnWork);
        assert_eq!(
            classified.classification,
            WorkItemClassification::AgentCommentary
        );
    }

    #[test]
    fn unknown_info_system_event_is_hidden_and_audited() {
        let item = info_system(Some("future_event"), "Future event", None);
        let classified = classify_turn_item_with_db_status(&item, Some("completed"));
        assert_eq!(classified.visibility, ProjectionVisibility::Hidden);
        assert_eq!(
            classified.classification,
            WorkItemClassification::UnknownSystemEvent
        );
        assert!(classified.audit);
    }
}
