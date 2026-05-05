use crate::constants::events;
use crate::{
    ContextCompressedNotification, ContextCompressingNotification, ItemCompletedNotification,
    ItemDeltaNotification, ItemDeltaStream, ItemRecoveryAttachedNotification,
    ItemRecoveryExhaustedNotification, ItemRecoveryOpenedNotification,
    ItemRecoverySucceededNotification, ItemRetryAttemptStartedNotification,
    ItemRetryScheduledNotification, ItemStartedNotification, ItemTimeoutDetectedNotification,
    ItemToolRetryExhaustedNotification, ItemToolRetryResolvedNotification,
    ItemToolRetryScheduledNotification, ItemUpdatedNotification, JsonRpcNotification,
    McpChangedNotification, McpServerCatalogChangedNotification,
    McpServerStatusChangedNotification, MemoryCandidateCreatedNotification,
    MemoryChangedNotification, MemoryForgottenNotification, SkillsChangedNotification,
    SkillsUploadChunkAckNotification, TaskCancelledNotification, TaskCompletedNotification,
    TaskCreatedNotification, TaskDeliveryCancelledNotification, TaskDeliveryDeliveredNotification,
    TaskDeliveryFailedNotification, TaskDeliveryQueuedNotification,
    TaskDeliveryStartedNotification, TaskDetachedNotification, TaskFailedNotification,
    TaskPausedNotification, TaskProgressNotification, TaskQueuedNotification,
    TaskRecoveredNotification, TaskRescheduledNotification, TaskResumedNotification,
    TaskRunCompletedNotification, TaskRunCreatedNotification, TaskRunFailedNotification,
    TaskRunStartedNotification, TaskScheduledNotification,
    TaskTreeChangedNotification as TaskTreeChangedTaskNotification, ThreadClosedNotification,
    ThreadStartedNotification, ThreadTreeChangedNotification, ThreadUpdatedNotification,
    TurnCompletedNotification, TurnFailedNotification, TurnStartedNotification,
    TurnTimelineChangedNotification, TurnToolLoopBudgetExceededNotification,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct UnknownGatewayNotification {
    pub method: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_id: Option<String>,
    pub params: JsonValue,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(tag = "kind", content = "params", rename_all = "snake_case")]
pub enum GatewayNotification {
    ThreadStarted(ThreadStartedNotification),
    ThreadClosed(ThreadClosedNotification),
    ThreadUpdated(ThreadUpdatedNotification),
    ThreadTreeChanged(ThreadTreeChangedNotification),
    TurnStarted(TurnStartedNotification),
    TurnCompleted(TurnCompletedNotification),
    TurnFailed(TurnFailedNotification),
    TurnTimelineChanged(TurnTimelineChangedNotification),
    ItemStarted(ItemStartedNotification),
    ItemDelta(ItemDeltaNotification),
    ItemTimeoutDetected(ItemTimeoutDetectedNotification),
    ItemRecoveryOpened(ItemRecoveryOpenedNotification),
    ItemRecoveryAttached(ItemRecoveryAttachedNotification),
    ItemRetryScheduled(ItemRetryScheduledNotification),
    ItemRetryAttemptStarted(ItemRetryAttemptStartedNotification),
    ItemRecoverySucceeded(ItemRecoverySucceededNotification),
    ItemRecoveryExhausted(ItemRecoveryExhaustedNotification),
    ItemToolRetryScheduled(ItemToolRetryScheduledNotification),
    ItemToolRetryResolved(ItemToolRetryResolvedNotification),
    ItemToolRetryExhausted(ItemToolRetryExhaustedNotification),
    ItemCompleted(ItemCompletedNotification),
    ItemUpdated(ItemUpdatedNotification),
    TurnToolLoopBudgetExceeded(TurnToolLoopBudgetExceededNotification),
    ContextCompressing(ContextCompressingNotification),
    ContextCompressed(ContextCompressedNotification),
    SkillsChanged(SkillsChangedNotification),
    SkillsUploadChunkAck(SkillsUploadChunkAckNotification),
    McpChanged(McpChangedNotification),
    McpServerStatusChanged(McpServerStatusChangedNotification),
    McpServerCatalogChanged(McpServerCatalogChangedNotification),
    TaskCreated(TaskCreatedNotification),
    TaskScheduled(TaskScheduledNotification),
    TaskQueued(TaskQueuedNotification),
    TaskRunCreated(TaskRunCreatedNotification),
    TaskRunStarted(TaskRunStartedNotification),
    TaskProgress(TaskProgressNotification),
    TaskRunCompleted(TaskRunCompletedNotification),
    TaskRunFailed(TaskRunFailedNotification),
    TaskRunCancelled(TaskRunFailedNotification),
    TaskCompleted(TaskCompletedNotification),
    TaskFailed(TaskFailedNotification),
    TaskCancelled(TaskCancelledNotification),
    TaskDetached(TaskDetachedNotification),
    TaskRescheduled(TaskRescheduledNotification),
    TaskPaused(TaskPausedNotification),
    TaskResumed(TaskResumedNotification),
    TaskDeliveryQueued(TaskDeliveryQueuedNotification),
    TaskDeliveryStarted(TaskDeliveryStartedNotification),
    TaskDeliveryDelivered(TaskDeliveryDeliveredNotification),
    TaskDeliveryFailed(TaskDeliveryFailedNotification),
    TaskDeliveryCancelled(TaskDeliveryCancelledNotification),
    TaskTreeChanged(TaskTreeChangedTaskNotification),
    TaskRecovered(TaskRecoveredNotification),
    MemoryChanged(MemoryChangedNotification),
    MemoryCandidateCreated(MemoryCandidateCreatedNotification),
    MemoryForgotten(MemoryForgottenNotification),
    Unknown(UnknownGatewayNotification),
}

impl GatewayNotification {
    pub fn from_jsonrpc(notification: JsonRpcNotification) -> Option<Self> {
        let method = notification.method;

        let params = notification.params?;

        match method.as_str() {
            events::THREAD_STARTED => serde_json::from_value::<ThreadStartedNotification>(params)
                .ok()
                .map(Self::ThreadStarted),
            events::THREAD_CLOSED => serde_json::from_value::<ThreadClosedNotification>(params)
                .ok()
                .map(Self::ThreadClosed),
            events::THREAD_UPDATED => serde_json::from_value::<ThreadUpdatedNotification>(params)
                .ok()
                .map(Self::ThreadUpdated),
            events::THREAD_TREE_CHANGED => {
                serde_json::from_value::<ThreadTreeChangedNotification>(params)
                    .ok()
                    .map(Self::ThreadTreeChanged)
            }
            events::TURN_STARTED => serde_json::from_value::<TurnStartedNotification>(params)
                .ok()
                .map(Self::TurnStarted),
            events::TURN_COMPLETED => serde_json::from_value::<TurnCompletedNotification>(params)
                .ok()
                .map(Self::TurnCompleted),
            events::TURN_FAILED => serde_json::from_value::<TurnFailedNotification>(params)
                .ok()
                .map(Self::TurnFailed),
            events::TURN_TIMELINE_CHANGED => {
                serde_json::from_value::<TurnTimelineChangedNotification>(params)
                    .ok()
                    .map(Self::TurnTimelineChanged)
            }
            events::ITEM_STARTED => {
                match serde_json::from_value::<ItemStartedNotification>(params.clone()) {
                    Ok(notification) => Some(Self::ItemStarted(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::ITEM_AGENT_MESSAGE_DELTA => {
                parse_item_delta_notification(params.clone(), ItemDeltaStream::AgentMessage, method)
            }
            events::ITEM_COMMAND_EXECUTION_OUTPUT_DELTA => {
                parse_item_delta_notification(params.clone(), ItemDeltaStream::Stdout, method)
            }
            events::ITEM_FILE_CHANGE_OUTPUT_DELTA => {
                parse_item_delta_notification(params.clone(), ItemDeltaStream::FileChange, method)
            }
            events::ITEM_TOOL_PROGRESS => {
                parse_item_delta_notification(params.clone(), ItemDeltaStream::ToolProgress, method)
            }
            events::ITEM_COMPLETED => {
                match serde_json::from_value::<ItemCompletedNotification>(params.clone()) {
                    Ok(notification) => Some(Self::ItemCompleted(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::ITEM_UPDATED => {
                match serde_json::from_value::<ItemUpdatedNotification>(params.clone()) {
                    Ok(notification) => Some(Self::ItemUpdated(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::ITEM_TIMEOUT_DETECTED => {
                match serde_json::from_value::<ItemTimeoutDetectedNotification>(params.clone()) {
                    Ok(notification) => Some(Self::ItemTimeoutDetected(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::ITEM_RECOVERY_OPENED => {
                match serde_json::from_value::<ItemRecoveryOpenedNotification>(params.clone()) {
                    Ok(notification) => Some(Self::ItemRecoveryOpened(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::ITEM_RECOVERY_ATTACHED => {
                match serde_json::from_value::<ItemRecoveryAttachedNotification>(params.clone()) {
                    Ok(notification) => Some(Self::ItemRecoveryAttached(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::ITEM_RETRY_SCHEDULED => {
                match serde_json::from_value::<ItemRetryScheduledNotification>(params.clone()) {
                    Ok(notification) => Some(Self::ItemRetryScheduled(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::ITEM_RETRY_ATTEMPT_STARTED => {
                match serde_json::from_value::<ItemRetryAttemptStartedNotification>(params.clone())
                {
                    Ok(notification) => Some(Self::ItemRetryAttemptStarted(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::ITEM_RECOVERY_SUCCEEDED => {
                match serde_json::from_value::<ItemRecoverySucceededNotification>(params.clone()) {
                    Ok(notification) => Some(Self::ItemRecoverySucceeded(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::ITEM_RECOVERY_EXHAUSTED => {
                match serde_json::from_value::<ItemRecoveryExhaustedNotification>(params.clone()) {
                    Ok(notification) => Some(Self::ItemRecoveryExhausted(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::ITEM_TOOL_RETRY_SCHEDULED => {
                match serde_json::from_value::<ItemToolRetryScheduledNotification>(params.clone()) {
                    Ok(notification) => Some(Self::ItemToolRetryScheduled(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::ITEM_TOOL_RETRY_RESOLVED => {
                match serde_json::from_value::<ItemToolRetryResolvedNotification>(params.clone()) {
                    Ok(notification) => Some(Self::ItemToolRetryResolved(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::ITEM_TOOL_RETRY_EXHAUSTED => {
                match serde_json::from_value::<ItemToolRetryExhaustedNotification>(params.clone()) {
                    Ok(notification) => Some(Self::ItemToolRetryExhausted(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::TURN_TOOL_LOOP_BUDGET_EXCEEDED => {
                match serde_json::from_value::<TurnToolLoopBudgetExceededNotification>(
                    params.clone(),
                ) {
                    Ok(notification) => Some(Self::TurnToolLoopBudgetExceeded(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::CONTEXT_COMPRESSING => {
                serde_json::from_value::<ContextCompressingNotification>(params)
                    .ok()
                    .map(Self::ContextCompressing)
            }
            events::CONTEXT_COMPRESSED => {
                serde_json::from_value::<ContextCompressedNotification>(params)
                    .ok()
                    .map(Self::ContextCompressed)
            }
            events::SKILLS_CHANGED => serde_json::from_value::<SkillsChangedNotification>(params)
                .ok()
                .map(Self::SkillsChanged),
            events::SKILLS_UPLOAD_CHUNK_ACK => {
                serde_json::from_value::<SkillsUploadChunkAckNotification>(params)
                    .ok()
                    .map(Self::SkillsUploadChunkAck)
            }
            events::MCP_CHANGED => serde_json::from_value::<McpChangedNotification>(params)
                .ok()
                .map(Self::McpChanged),
            events::MCP_SERVER_STATUS_CHANGED => {
                serde_json::from_value::<McpServerStatusChangedNotification>(params)
                    .ok()
                    .map(Self::McpServerStatusChanged)
            }
            events::MCP_SERVER_CATALOG_CHANGED => {
                serde_json::from_value::<McpServerCatalogChangedNotification>(params)
                    .ok()
                    .map(Self::McpServerCatalogChanged)
            }
            events::TASK_CREATED => serde_json::from_value::<TaskCreatedNotification>(params)
                .ok()
                .map(Self::TaskCreated),
            events::TASK_SCHEDULED => serde_json::from_value::<TaskScheduledNotification>(params)
                .ok()
                .map(Self::TaskScheduled),
            events::TASK_QUEUED => serde_json::from_value::<TaskQueuedNotification>(params)
                .ok()
                .map(Self::TaskQueued),
            events::TASK_RUN_CREATED => {
                serde_json::from_value::<TaskRunCreatedNotification>(params)
                    .ok()
                    .map(Self::TaskRunCreated)
            }
            events::TASK_RUN_STARTED => {
                serde_json::from_value::<TaskRunStartedNotification>(params)
                    .ok()
                    .map(Self::TaskRunStarted)
            }
            events::TASK_PROGRESS => serde_json::from_value::<TaskProgressNotification>(params)
                .ok()
                .map(Self::TaskProgress),
            events::TASK_RUN_COMPLETED => {
                serde_json::from_value::<TaskRunCompletedNotification>(params)
                    .ok()
                    .map(Self::TaskRunCompleted)
            }
            events::TASK_RUN_FAILED => serde_json::from_value::<TaskRunFailedNotification>(params)
                .ok()
                .map(Self::TaskRunFailed),
            events::TASK_RUN_CANCELLED => {
                serde_json::from_value::<TaskRunFailedNotification>(params)
                    .ok()
                    .map(Self::TaskRunCancelled)
            }
            events::TASK_COMPLETED => serde_json::from_value::<TaskCompletedNotification>(params)
                .ok()
                .map(Self::TaskCompleted),
            events::TASK_FAILED => serde_json::from_value::<TaskFailedNotification>(params)
                .ok()
                .map(Self::TaskFailed),
            events::TASK_CANCELLED => serde_json::from_value::<TaskCancelledNotification>(params)
                .ok()
                .map(Self::TaskCancelled),
            events::TASK_DETACHED => serde_json::from_value::<TaskDetachedNotification>(params)
                .ok()
                .map(Self::TaskDetached),
            events::TASK_RESCHEDULED => {
                serde_json::from_value::<TaskRescheduledNotification>(params)
                    .ok()
                    .map(Self::TaskRescheduled)
            }
            events::TASK_PAUSED => serde_json::from_value::<TaskPausedNotification>(params)
                .ok()
                .map(Self::TaskPaused),
            events::TASK_RESUMED => serde_json::from_value::<TaskResumedNotification>(params)
                .ok()
                .map(Self::TaskResumed),
            events::TASK_DELIVERY_QUEUED => {
                serde_json::from_value::<TaskDeliveryQueuedNotification>(params)
                    .ok()
                    .map(Self::TaskDeliveryQueued)
            }
            events::TASK_DELIVERY_STARTED => {
                serde_json::from_value::<TaskDeliveryStartedNotification>(params)
                    .ok()
                    .map(Self::TaskDeliveryStarted)
            }
            events::TASK_DELIVERY_DELIVERED => {
                serde_json::from_value::<TaskDeliveryDeliveredNotification>(params)
                    .ok()
                    .map(Self::TaskDeliveryDelivered)
            }
            events::TASK_DELIVERY_FAILED => {
                serde_json::from_value::<TaskDeliveryFailedNotification>(params)
                    .ok()
                    .map(Self::TaskDeliveryFailed)
            }
            events::TASK_DELIVERY_CANCELLED => {
                serde_json::from_value::<TaskDeliveryCancelledNotification>(params)
                    .ok()
                    .map(Self::TaskDeliveryCancelled)
            }
            events::TASK_TREE_CHANGED => {
                serde_json::from_value::<TaskTreeChangedTaskNotification>(params)
                    .ok()
                    .map(Self::TaskTreeChanged)
            }
            events::TASK_RECOVERED => serde_json::from_value::<TaskRecoveredNotification>(params)
                .ok()
                .map(Self::TaskRecovered),
            events::MEMORY_CHANGED => {
                match serde_json::from_value::<MemoryChangedNotification>(params.clone()) {
                    Ok(notification) => Some(Self::MemoryChanged(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::MEMORY_CANDIDATE_CREATED => {
                match serde_json::from_value::<MemoryCandidateCreatedNotification>(params.clone()) {
                    Ok(notification) => Some(Self::MemoryCandidateCreated(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            events::MEMORY_FORGOTTEN => {
                match serde_json::from_value::<MemoryForgottenNotification>(params.clone()) {
                    Ok(notification) => Some(Self::MemoryForgotten(notification)),
                    Err(_) => Some(Self::Unknown(unknown_notification(method, params))),
                }
            }
            _ if method.starts_with("item/")
                || method.starts_with("turn/")
                || method.starts_with("context/")
                || method.starts_with("task/")
                || method.starts_with("memory/") =>
            {
                Some(Self::Unknown(unknown_notification(method, params)))
            }
            _ => None,
        }
    }
}

fn parse_item_delta_notification(
    params: JsonValue,
    default_stream: ItemDeltaStream,
    method: String,
) -> Option<GatewayNotification> {
    match serde_json::from_value::<ItemDeltaNotification>(params.clone()) {
        Ok(mut notification) => {
            if notification.stream.is_none() {
                notification.stream = Some(default_stream);
            }
            Some(GatewayNotification::ItemDelta(notification))
        }
        Err(_) => Some(GatewayNotification::Unknown(unknown_notification(
            method, params,
        ))),
    }
}

fn unknown_notification(method: String, params: JsonValue) -> UnknownGatewayNotification {
    let (workspace_id, thread_id, turn_id, item_id) =
        extract_workspace_thread_turn_item(params.as_object());

    UnknownGatewayNotification {
        method,
        workspace_id,
        thread_id,
        turn_id,
        item_id,
        params,
    }
}

fn extract_workspace_thread_turn_item(
    object: Option<&serde_json::Map<String, JsonValue>>,
) -> (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
) {
    let Some(object) = object else {
        return (None, None, None, None);
    };

    let workspace_id = object
        .get("workspace_id")
        .and_then(JsonValue::as_str)
        .map(str::to_owned);
    let thread_id = object
        .get("thread_id")
        .and_then(JsonValue::as_str)
        .map(str::to_owned);
    let turn_id = object
        .get("turn_id")
        .and_then(JsonValue::as_str)
        .map(str::to_owned);
    let item_id = object
        .get("item_id")
        .and_then(JsonValue::as_str)
        .map(str::to_owned);

    (workspace_id, thread_id, turn_id, item_id)
}

#[cfg(test)]
mod tests {
    use super::GatewayNotification;
    use crate::{
        ItemDeltaStream, JsonRpcNotification, MemoryCandidateCreatedNotification,
        MemoryChangedNotification, MemoryForgottenNotification, RecoveryAction, RecoveryJobStatus,
        RecoveryTrigger, ToolLoopBudgetAction, ToolLoopBudgetLimitKind, ToolRetryErrorClass,
        ToolRetryExhaustionKind, ToolRetryResolution, TurnItemType,
    };
    use serde_json::json;

    #[test]
    fn maps_known_notification() {
        let notification: JsonRpcNotification = serde_json::from_value(json!({
            "jsonrpc": "2.0",
            "method": "turn/started",
            "params": {
                "workspace_id": "ws_123",
                "thread_id": "thr_123",
                "turn": {
                    "id": "turn_123",
                    "status": "InProgress"
                }
            }
        }))
        .expect("notification should decode");

        let mapped =
            GatewayNotification::from_jsonrpc(notification).expect("known notification should map");
        assert!(matches!(mapped, GatewayNotification::TurnStarted(_)));
    }

    #[test]
    fn maps_memory_notifications() {
        let changed = JsonRpcNotification::from_params(
            "memory/changed",
            &json!({
                "memory_id": "mem_1",
                "scope": {
                    "kind": "workspace",
                    "key": "ws_1"
                },
                "change_kind": "created"
            }),
        )
        .expect("memory changed notification should encode");
        let mapped = GatewayNotification::from_jsonrpc(changed).expect("memory changed should map");
        assert!(matches!(
            mapped,
            GatewayNotification::MemoryChanged(MemoryChangedNotification { .. })
        ));

        let candidate_created = JsonRpcNotification::from_params(
            "memory/candidate_created",
            &json!({
                "candidate": {
                    "id": "cand_1",
                    "scope": {
                        "kind": "workspace",
                        "key": "ws_1"
                    },
                    "category": "preference",
                    "candidate_text": "The user likes compact summaries.",
                    "confidence": 0.8,
                    "reason": "explicit statement",
                    "provenance": {
                        "source_kind": "explicit_user_request"
                    },
                    "status": "pending",
                    "created_at": 1700000000
                }
            }),
        )
        .expect("candidate created notification should encode");
        let mapped = GatewayNotification::from_jsonrpc(candidate_created)
            .expect("candidate created should map");
        assert!(matches!(
            mapped,
            GatewayNotification::MemoryCandidateCreated(MemoryCandidateCreatedNotification { .. })
        ));

        let forgotten = JsonRpcNotification::from_params(
            "memory/forgotten",
            &json!({
                "memory_ids": ["mem_1"],
                "reason": "user request"
            }),
        )
        .expect("memory forgotten notification should encode");
        let mapped =
            GatewayNotification::from_jsonrpc(forgotten).expect("memory forgotten should map");
        assert!(matches!(
            mapped,
            GatewayNotification::MemoryForgotten(MemoryForgottenNotification { .. })
        ));
    }

    #[test]
    fn maps_thread_updated_notification() {
        let notification: JsonRpcNotification = serde_json::from_value(json!({
            "jsonrpc": "2.0",
            "method": "thread/updated",
            "params": {
                "thread": {
                    "workspace_id": "ws_123",
                    "id": "thr_123",
                    "name": "First title",
                    "preview": "",
                    "mode": "Chat",
                    "model": "gpt-5.4",
                    "model_provider": "openai",
                    "created_at": 0,
                    "updated_at": 0,
                    "status": "Idle",
                    "turns": []
                }
            }
        }))
        .expect("notification should decode");

        let mapped =
            GatewayNotification::from_jsonrpc(notification).expect("known notification should map");
        assert!(matches!(mapped, GatewayNotification::ThreadUpdated(_)));
    }

    #[test]
    fn maps_malformed_item_notification_to_unknown() {
        let notification: JsonRpcNotification = serde_json::from_value(json!({
            "jsonrpc": "2.0",
            "method": "item/completed",
            "params": {
                "workspace_id": "ws_123",
                "thread_id": "thr_123",
                "turn_id": "turn_123"
            }
        }))
        .expect("notification should decode");

        let mapped =
            GatewayNotification::from_jsonrpc(notification).expect("malformed item should map");
        assert!(matches!(mapped, GatewayNotification::Unknown(_)));
    }

    #[test]
    fn maps_item_recovery_opened_notification() {
        let notification: JsonRpcNotification = serde_json::from_value(json!({
            "jsonrpc": "2.0",
            "method": "item/recovery_opened",
            "params": {
                "workspace_id": "ws_123",
                "thread_id": "thr_123",
                "turn_id": "turn_123",
                "item_id": "item_123",
                "item_type": "agent_message",
                "recovery_job_id": "rec_123",
                "trigger": "provider_error",
                "action": "retry_with_backoff",
                "attempt_number": 1
            }
        }))
        .expect("notification should decode");

        let mapped =
            GatewayNotification::from_jsonrpc(notification).expect("recovery opened should map");
        match mapped {
            GatewayNotification::ItemRecoveryOpened(notification) => {
                assert_eq!(notification.workspace_id, "ws_123");
                assert_eq!(notification.thread_id, "thr_123");
                assert_eq!(notification.turn_id, "turn_123");
                assert_eq!(notification.item_id, "item_123");
                assert_eq!(notification.item_type, TurnItemType::AgentMessage);
                assert_eq!(notification.recovery_job_id, "rec_123");
                assert_eq!(notification.trigger, RecoveryTrigger::ProviderError);
                assert_eq!(notification.action, RecoveryAction::RetryWithBackoff);
                assert_eq!(notification.attempt_number, 1);
            }
            other => panic!("expected item recovery opened, got {other:?}"),
        }
    }

    #[test]
    fn maps_item_recovery_attached_notification() {
        let notification: JsonRpcNotification = serde_json::from_value(json!({
            "jsonrpc": "2.0",
            "method": "item/recovery_attached",
            "params": {
                "workspace_id": "ws_123",
                "thread_id": "thr_123",
                "turn_id": "turn_123",
                "item_id": "item_456",
                "item_type": "command_execution",
                "recovery_job_id": "rec_123",
                "recovery_item_id": "item_123",
                "recovery_item_type": "agent_message",
                "trigger": "timeout",
                "action": "retry_attempt",
                "existing_status": "active",
                "next_attempt_number": 2
            }
        }))
        .expect("notification should decode");

        let mapped =
            GatewayNotification::from_jsonrpc(notification).expect("recovery attached should map");
        match mapped {
            GatewayNotification::ItemRecoveryAttached(notification) => {
                assert_eq!(notification.workspace_id, "ws_123");
                assert_eq!(notification.thread_id, "thr_123");
                assert_eq!(notification.turn_id, "turn_123");
                assert_eq!(notification.item_id, "item_456");
                assert_eq!(notification.item_type, TurnItemType::CommandExecution);
                assert_eq!(notification.recovery_job_id, "rec_123");
                assert_eq!(notification.recovery_item_id, "item_123");
                assert_eq!(notification.recovery_item_type, TurnItemType::AgentMessage);
                assert_eq!(notification.trigger, RecoveryTrigger::Timeout);
                assert_eq!(notification.action, RecoveryAction::RetryAttempt);
                assert_eq!(notification.existing_status, RecoveryJobStatus::Active);
                assert_eq!(notification.next_attempt_number, 2);
            }
            other => panic!("expected item recovery attached, got {other:?}"),
        }
    }

    #[test]
    fn maps_item_tool_retry_scheduled_notification() {
        let notification: JsonRpcNotification = serde_json::from_value(json!({
            "jsonrpc": "2.0",
            "method": "item/tool/retry_scheduled",
            "params": {
                "workspace_id": "ws_123",
                "thread_id": "thr_123",
                "turn_id": "turn_123",
                "item_id": "item_tool_123",
                "item_type": "web_fetch",
                "tool_retry_episode_id": "tool_retry_turn_123_1",
                "tool_name": "web_fetch",
                "attempt_number": 2,
                "error_class": "timeout",
                "retry_hint": "retry with a smaller request",
                "budgets": [
                    {"kind": "episode", "used": 1, "limit": 3}
                ],
                "failure_signature_fingerprint": "sig_123",
                "reason": "recoverable_tool_output"
            }
        }))
        .expect("notification should decode");

        let mapped = GatewayNotification::from_jsonrpc(notification)
            .expect("tool retry scheduled should map");
        match mapped {
            GatewayNotification::ItemToolRetryScheduled(notification) => {
                assert_eq!(notification.workspace_id, "ws_123");
                assert_eq!(notification.item_type, TurnItemType::WebFetch);
                assert_eq!(notification.error_class, ToolRetryErrorClass::Timeout);
                assert_eq!(notification.budgets.len(), 1);
                assert_eq!(notification.budgets[0].used, 1);
            }
            other => panic!("expected item tool retry scheduled, got {other:?}"),
        }
    }

    #[test]
    fn maps_item_tool_retry_resolved_notification() {
        let notification: JsonRpcNotification = serde_json::from_value(json!({
            "jsonrpc": "2.0",
            "method": "item/tool/retry_resolved",
            "params": {
                "workspace_id": "ws_123",
                "thread_id": "thr_123",
                "turn_id": "turn_123",
                "item_id": "item_tool_123",
                "item_type": "web_fetch",
                "tool_retry_episode_id": "tool_retry_turn_123_1",
                "tool_name": "web_fetch",
                "attempt_number": 3,
                "resolution": "succeeded",
                "budgets": [
                    {"kind": "episode", "used": 1, "limit": 3}
                ],
                "reason": "successful_tool_output"
            }
        }))
        .expect("notification should decode");

        let mapped = GatewayNotification::from_jsonrpc(notification)
            .expect("tool retry resolved should map");
        match mapped {
            GatewayNotification::ItemToolRetryResolved(notification) => {
                assert_eq!(notification.item_id, "item_tool_123");
                assert_eq!(notification.resolution, ToolRetryResolution::Succeeded);
                assert_eq!(notification.budgets[0].limit, 3);
            }
            other => panic!("expected item tool retry resolved, got {other:?}"),
        }
    }

    #[test]
    fn maps_item_tool_retry_exhausted_notification() {
        let notification: JsonRpcNotification = serde_json::from_value(json!({
            "jsonrpc": "2.0",
            "method": "item/tool/retry_exhausted",
            "params": {
                "workspace_id": "ws_123",
                "thread_id": "thr_123",
                "turn_id": "turn_123",
                "item_id": "item_tool_123",
                "item_type": "web_fetch",
                "tool_retry_episode_id": "tool_retry_turn_123_1",
                "tool_name": "web_fetch",
                "attempt_number": 4,
                "error_class": "timeout",
                "exhaustion_kind": "failure_signature",
                "budgets": [
                    {"kind": "failure_signature", "used": 2, "limit": 2}
                ],
                "failure_signature_fingerprint": "sig_123",
                "reason": "same_failure_signature"
            }
        }))
        .expect("notification should decode");

        let mapped = GatewayNotification::from_jsonrpc(notification)
            .expect("tool retry exhausted should map");
        match mapped {
            GatewayNotification::ItemToolRetryExhausted(notification) => {
                assert_eq!(
                    notification.exhaustion_kind,
                    ToolRetryExhaustionKind::FailureSignature
                );
                assert_eq!(notification.failure_signature_fingerprint, "sig_123");
            }
            other => panic!("expected item tool retry exhausted, got {other:?}"),
        }
    }

    #[test]
    fn maps_turn_tool_loop_budget_exceeded_notification() {
        let notification: JsonRpcNotification = serde_json::from_value(json!({
            "jsonrpc": "2.0",
            "method": "turn/tool_loop/budget_exceeded",
            "params": {
                "workspace_id": "ws_123",
                "thread_id": "thr_123",
                "turn_id": "turn_123",
                "limit_kind": "agent_rounds",
                "limit": 32,
                "observed": 33,
                "action": "request_final_no_tools_round",
                "reason": "agent_rounds_exceeded"
            }
        }))
        .expect("notification should decode");

        let mapped = GatewayNotification::from_jsonrpc(notification)
            .expect("tool loop budget notification should map");
        match mapped {
            GatewayNotification::TurnToolLoopBudgetExceeded(notification) => {
                assert_eq!(
                    notification.limit_kind,
                    ToolLoopBudgetLimitKind::AgentRounds
                );
                assert_eq!(
                    notification.action,
                    ToolLoopBudgetAction::RequestFinalNoToolsRound
                );
                assert_eq!(notification.observed, 33);
            }
            other => panic!("expected tool loop budget exceeded, got {other:?}"),
        }
    }

    #[test]
    fn maps_malformed_tool_retry_notification_to_unknown() {
        let notification: JsonRpcNotification = serde_json::from_value(json!({
            "jsonrpc": "2.0",
            "method": "item/tool/retry_scheduled",
            "params": {
                "workspace_id": "ws_123",
                "thread_id": "thr_123",
                "turn_id": "turn_123"
            }
        }))
        .expect("notification should decode");

        let mapped =
            GatewayNotification::from_jsonrpc(notification).expect("malformed item should map");
        assert!(matches!(mapped, GatewayNotification::Unknown(_)));
    }

    #[test]
    fn schema_documents_include_tool_retry_notifications_and_replay_payloads() {
        let schema_names = crate::protocol_schema_documents()
            .into_iter()
            .map(|document| document.file_name)
            .collect::<Vec<_>>();

        for expected in [
            "item_tool_retry_scheduled_notification.json",
            "item_tool_retry_resolved_notification.json",
            "item_tool_retry_exhausted_notification.json",
            "turn_tool_loop_budget_exceeded_notification.json",
            "turn_item_event_payload.json",
            "thread_history_event_payload.json",
            "tool_recovery_policy_snapshot.json",
            "tool_output_policy_snapshot.json",
            "tool_output_summary.json",
            "tool_display_payload.json",
            "tool_storage_payload.json",
            "tool_recovery_view.json",
            "tool_recovery_retry_class.json",
            "tool_recovery_idempotency_mode.json",
            "gateway_notification.json",
            "mcp_list_params.json",
            "mcp_list_response.json",
            "mcp_scope_kind.json",
            "mcp_source_kind.json",
            "mcp_install_params.json",
            "mcp_install_response.json",
            "mcp_install_status.json",
            "mcp_install_result_status.json",
            "mcp_policy_state.json",
            "mcp_policy_set_params.json",
            "mcp_policy_set_response.json",
            "mcp_changed_notification.json",
            "mcp_changed_action.json",
            "mcp_server_status_changed_notification.json",
            "mcp_server_catalog_changed_notification.json",
            "task.json",
            "task_run.json",
            "task_agent_spec.json",
            "task_event.json",
            "thread_lineage.json",
            "task_turn_item.json",
            "task_created_notification.json",
            "task_run_started_notification.json",
            "task_completed_notification.json",
            "thread_origin_kind.json",
            "thread_sidebar_visibility.json",
        ] {
            assert!(
                schema_names.iter().any(|name| *name == expected),
                "missing schema document {expected}"
            );
        }
    }

    #[test]
    fn keeps_explicit_generic_stream_on_item_agent_message_delta() {
        let notification: JsonRpcNotification = serde_json::from_value(json!({
            "jsonrpc": "2.0",
            "method": "item/agent_message/delta",
            "params": {
                "workspace_id": "ws_123",
                "thread_id": "thr_123",
                "turn_id": "turn_123",
                "item_id": "item_123",
                "delta": "reasoning chunk",
                "stream": "generic"
            }
        }))
        .expect("notification should decode");

        let mapped =
            GatewayNotification::from_jsonrpc(notification).expect("item delta should map");
        match mapped {
            GatewayNotification::ItemDelta(notification) => {
                assert_eq!(notification.stream, Some(ItemDeltaStream::Generic));
            }
            other => panic!("expected item delta, got {other:?}"),
        }
    }

    #[test]
    fn ignores_irrelevant_notification_methods() {
        let notification: JsonRpcNotification = serde_json::from_value(json!({
            "jsonrpc": "2.0",
            "method": "system/health",
            "params": {}
        }))
        .expect("notification should decode");

        assert!(GatewayNotification::from_jsonrpc(notification).is_none());
    }
}
