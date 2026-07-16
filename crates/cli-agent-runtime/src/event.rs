//! Canonical CLI agent runtime events and Codex native event mapping.

use crate::codex::{CodexJsonlRpcNotificationEvent, CodexJsonlRpcServerRequest};
use pioneer_runtime_events::{OrderedIngressClass, OrderedIngressEvent};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RuntimeEventMappingOptions {
    pub include_redacted_native_payload: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RuntimeEvent {
    SessionStateChanged(RuntimeSessionStateChanged),
    ThreadStateChanged(RuntimeThreadStateChanged),
    TurnStarted(RuntimeTurnStarted),
    TurnCompleted(RuntimeTurnCompleted),
    TurnFailed(RuntimeTurnFailed),
    TurnInterrupted(RuntimeTurnInterrupted),
    TurnRetrying(RuntimeTurnRetrying),
    ItemStarted(RuntimeItemStarted),
    ItemDelta(RuntimeItemDelta),
    ItemCompleted(RuntimeItemCompleted),
    ItemUpdated(RuntimeItemUpdated),
    PlanUpdated(RuntimePlanUpdated),
    DiffUpdated(RuntimeDiffUpdated),
    RequestOpened(RuntimeRequestOpened),
    RequestResolved(RuntimeRequestResolved),
    AccountUpdated(RuntimeAccountUpdated),
    AppListUpdated(RuntimeAppListUpdated),
    ReviewModeChanged(RuntimeReviewModeChanged),
    Error(RuntimeErrorEvent),
    Raw(RuntimeRawEvent),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeTurnTerminalKind {
    Completed,
    Failed,
    Interrupted,
}

impl RuntimeEvent {
    pub const fn turn_terminal_kind(&self) -> Option<RuntimeTurnTerminalKind> {
        match self {
            Self::TurnCompleted(_) => Some(RuntimeTurnTerminalKind::Completed),
            Self::TurnFailed(_) => Some(RuntimeTurnTerminalKind::Failed),
            Self::TurnInterrupted(_) => Some(RuntimeTurnTerminalKind::Interrupted),
            Self::Error(error) if !error.retryable && error.native_turn_id.is_some() => {
                Some(RuntimeTurnTerminalKind::Failed)
            }
            _ => None,
        }
    }
}

const RUNTIME_EVENT_MAX_COALESCED_DELTA_BYTES: usize = 64 * 1024;

impl OrderedIngressEvent for RuntimeEvent {
    type Key = String;

    fn ingress_class(&self) -> OrderedIngressClass<Self::Key> {
        let key = match self {
            Self::ItemDelta(delta) => format!(
                "item:{:?}:{}:{}:{}",
                delta.delta_kind,
                delta.native_thread_id.as_deref().unwrap_or_default(),
                delta.native_turn_id,
                delta.native_item_id,
            ),
            Self::ItemUpdated(updated) => format!(
                "item_updated:{}:{}:{}",
                updated.native_thread_id.as_deref().unwrap_or_default(),
                updated.native_turn_id,
                updated.native_item_id,
            ),
            Self::PlanUpdated(plan) => format!(
                "plan:{}:{}",
                plan.native_thread_id.as_deref().unwrap_or_default(),
                plan.native_turn_id,
            ),
            Self::DiffUpdated(diff) => format!(
                "diff:{}:{}",
                diff.native_thread_id.as_deref().unwrap_or_default(),
                diff.native_turn_id,
            ),
            Self::AccountUpdated(_) => "account".to_owned(),
            Self::AppListUpdated(_) => "apps".to_owned(),
            _ => return OrderedIngressClass::Durable,
        };
        OrderedIngressClass::Coalesced(key)
    }

    fn coalesce(&mut self, newer: Self) {
        match (self, newer) {
            (Self::ItemDelta(current), Self::ItemDelta(mut next)) => {
                if matches!(current.delta_kind, RuntimeItemDeltaKind::ToolProgress) {
                    current.delta = next.delta;
                } else {
                    current.delta.push_str(next.delta.as_str());
                    truncate_runtime_event_delta(&mut current.delta);
                }
                if next.metadata.is_some() {
                    current.metadata = next.metadata.take();
                }
                if next.native.is_some() {
                    current.native = next.native.take();
                }
            }
            (current, newer) => *current = newer,
        }
    }
}

fn truncate_runtime_event_delta(delta: &mut String) {
    if delta.len() <= RUNTIME_EVENT_MAX_COALESCED_DELTA_BYTES {
        return;
    }
    const SUFFIX: &str = "\n[progress truncated]";
    let mut boundary = RUNTIME_EVENT_MAX_COALESCED_DELTA_BYTES.saturating_sub(SUFFIX.len());
    while boundary > 0 && !delta.is_char_boundary(boundary) {
        boundary -= 1;
    }
    delta.truncate(boundary);
    delta.push_str(SUFFIX);
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeNativeEvent {
    pub method: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_redacted: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_redacted: Option<JsonValue>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeSessionStateChanged {
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native: Option<RuntimeNativeEvent>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeThreadStateChanged {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_thread_id: Option<String>,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native: Option<RuntimeNativeEvent>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeTurnStarted {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_thread_id: Option<String>,
    pub native_turn_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native: Option<RuntimeNativeEvent>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeTurnCompleted {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_thread_id: Option<String>,
    pub native_turn_id: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native: Option<RuntimeNativeEvent>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeTurnFailed {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_turn_id: Option<String>,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native: Option<RuntimeNativeEvent>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeTurnInterrupted {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_thread_id: Option<String>,
    pub native_turn_id: String,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native: Option<RuntimeNativeEvent>,
}

/// A transient provider failure that the runtime is already retrying.
///
/// This event is diagnostic and never transfers turn ownership to recovery.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeTurnRetrying {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_turn_id: Option<String>,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native: Option<RuntimeNativeEvent>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeAgentMessagePhase {
    #[default]
    FinalAnswer,
    Commentary,
}

impl RuntimeAgentMessagePhase {
    pub fn is_final_answer(&self) -> bool {
        matches!(self, Self::FinalAnswer)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeItemStarted {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_thread_id: Option<String>,
    pub native_turn_id: String,
    pub native_item_id: String,
    pub item_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "RuntimeAgentMessagePhase::is_final_answer"
    )]
    pub phase: RuntimeAgentMessagePhase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_item_redacted: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native: Option<RuntimeNativeEvent>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeItemDelta {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_thread_id: Option<String>,
    pub native_turn_id: String,
    pub native_item_id: String,
    pub item_kind: String,
    pub delta_kind: RuntimeItemDeltaKind,
    pub delta: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native: Option<RuntimeNativeEvent>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeItemDeltaKind {
    AgentMessage,
    ReasoningText,
    ReasoningSummary,
    Plan,
    Stdout,
    Stderr,
    FileChange,
    ToolProgress,
    Generic,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeItemCompleted {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_thread_id: Option<String>,
    pub native_turn_id: String,
    pub native_item_id: String,
    pub item_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub summary: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub content: Vec<String>,
    #[serde(
        default,
        skip_serializing_if = "RuntimeAgentMessagePhase::is_final_answer"
    )]
    pub phase: RuntimeAgentMessagePhase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_item_redacted: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native: Option<RuntimeNativeEvent>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeItemUpdated {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_thread_id: Option<String>,
    pub native_turn_id: String,
    pub native_item_id: String,
    pub item_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native: Option<RuntimeNativeEvent>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimePlanUpdated {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_thread_id: Option<String>,
    pub native_turn_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_redacted: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native: Option<RuntimeNativeEvent>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeDiffUpdated {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_thread_id: Option<String>,
    pub native_turn_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diff_redacted: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native: Option<RuntimeNativeEvent>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeRequestOpened {
    pub native_request_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_request_id_json: Option<JsonValue>,
    pub request_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_item_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_redacted: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native: Option<RuntimeNativeEvent>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeRequestResolved {
    pub native_request_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native: Option<RuntimeNativeEvent>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeAccountUpdated {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native: Option<RuntimeNativeEvent>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeAppListUpdated {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native: Option<RuntimeNativeEvent>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeReviewModeChanged {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_turn_id: Option<String>,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native: Option<RuntimeNativeEvent>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeErrorEvent {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_turn_id: Option<String>,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native: Option<RuntimeNativeEvent>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeRawEvent {
    pub native_method: String,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_item_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_redacted: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_redacted: Option<JsonValue>,
}

pub fn map_codex_notification_event(
    notification: &CodexJsonlRpcNotificationEvent,
    options: RuntimeEventMappingOptions,
) -> RuntimeEvent {
    let params = notification.params.as_ref();
    match notification.method.as_str() {
        "thread/started" => RuntimeEvent::ThreadStateChanged(RuntimeThreadStateChanged {
            native_thread_id: params.and_then(|params| string_path(params, &["thread", "id"])),
            status: "started".to_owned(),
            native: native_notification(notification, options),
        }),
        "thread/status/changed" => RuntimeEvent::ThreadStateChanged(RuntimeThreadStateChanged {
            native_thread_id: params.and_then(|params| string_path(params, &["threadId"])),
            status: params
                .and_then(|params| string_path(params, &["status"]))
                .unwrap_or_else(|| "changed".to_owned()),
            native: native_notification(notification, options),
        }),
        "thread/closed" => RuntimeEvent::ThreadStateChanged(RuntimeThreadStateChanged {
            native_thread_id: params.and_then(|params| string_path(params, &["threadId"])),
            status: "closed".to_owned(),
            native: native_notification(notification, options),
        }),
        "thread/archived" => RuntimeEvent::ThreadStateChanged(RuntimeThreadStateChanged {
            native_thread_id: params.and_then(|params| string_path(params, &["threadId"])),
            status: "archived".to_owned(),
            native: native_notification(notification, options),
        }),
        "thread/unarchived" => RuntimeEvent::ThreadStateChanged(RuntimeThreadStateChanged {
            native_thread_id: params.and_then(|params| string_path(params, &["threadId"])),
            status: "unarchived".to_owned(),
            native: native_notification(notification, options),
        }),
        "thread/tokenUsage/updated"
        | "fuzzyFileSearch/sessionUpdated"
        | "fuzzyFileSearch/sessionCompleted"
        | "windowsSandbox/setupCompleted" => raw_notification_with_payload(
            notification,
            options,
            "runtime notification without required turn id",
        ),
        "turn/started" => map_codex_turn_started(notification, options),
        "turn/completed" => map_codex_turn_completed(notification, options),
        "error" => map_codex_error(notification, options),
        "turn/plan/updated" => map_codex_plan_updated(notification, options),
        "turn/diff/updated" => map_codex_diff_updated(notification, options),
        "item/started" => map_codex_item_started(notification, options),
        "item/completed" => map_codex_item_completed(notification, options),
        "item/agentMessage/delta" => map_codex_item_delta(
            notification,
            options,
            "agentMessage",
            RuntimeItemDeltaKind::AgentMessage,
        ),
        "item/reasoning/textDelta" => map_codex_item_delta(
            notification,
            options,
            "reasoning",
            RuntimeItemDeltaKind::ReasoningText,
        ),
        "item/reasoning/summaryTextDelta" => map_codex_item_delta(
            notification,
            options,
            "reasoning",
            RuntimeItemDeltaKind::ReasoningSummary,
        ),
        "item/reasoning/summaryPartAdded" => {
            map_codex_reasoning_summary_part_added(notification, options)
        }
        "item/plan/delta" => {
            map_codex_item_delta(notification, options, "plan", RuntimeItemDeltaKind::Plan)
        }
        "item/commandExecution/outputDelta" => map_codex_item_delta(
            notification,
            options,
            "commandExecution",
            command_delta_kind(params),
        ),
        "item/fileChange/outputDelta" | "item/fileChange/patchUpdated" => map_codex_item_delta(
            notification,
            options,
            "fileChange",
            RuntimeItemDeltaKind::FileChange,
        ),
        "item/mcpToolCall/progress" => map_codex_item_delta(
            notification,
            options,
            "mcpToolCall",
            RuntimeItemDeltaKind::ToolProgress,
        ),
        "serverRequest/resolved" => map_codex_request_resolved(notification, options),
        "account/updated" | "account/login/completed" => {
            RuntimeEvent::AccountUpdated(RuntimeAccountUpdated {
                native: native_notification(notification, options),
            })
        }
        "apps/changed" => RuntimeEvent::AppListUpdated(RuntimeAppListUpdated {
            native: native_notification(notification, options),
        }),
        "enteredReviewMode" | "review/entered" => {
            map_codex_review_mode_changed(notification, options, "entered")
        }
        "exitedReviewMode" | "review/exited" => {
            map_codex_review_mode_changed(notification, options, "exited")
        }
        _ => raw_notification(notification, options, "unsupported codex notification"),
    }
}

pub fn map_codex_server_request_event(
    request: &CodexJsonlRpcServerRequest,
    options: RuntimeEventMappingOptions,
) -> RuntimeEvent {
    let params = request.params.as_ref();
    let request_kind = match request.method.as_str() {
        "item/commandExecution/requestApproval" => "command_approval",
        "item/fileChange/requestApproval" => "file_change_approval",
        "item/permissions/requestApproval" => "permission_approval",
        "item/tool/requestUserInput" | "tool/requestUserInput" | "userInput/request" => {
            "user_input"
        }
        _ => {
            return RuntimeEvent::Raw(RuntimeRawEvent {
                native_method: request.method.clone(),
                reason: "unsupported codex server request".to_owned(),
                native_thread_id: params.and_then(|params| string_path(params, &["threadId"])),
                native_turn_id: params.and_then(|params| string_path(params, &["turnId"])),
                native_item_id: params.and_then(|params| string_path(params, &["itemId"])),
                payload_redacted: include_redacted(options, request.params.as_ref()),
                raw_redacted: include_redacted(options, Some(&request.raw)),
            });
        }
    };
    RuntimeEvent::RequestOpened(RuntimeRequestOpened {
        native_request_id: request.id.to_string(),
        native_request_id_json: serde_json::to_value(&request.id).ok(),
        request_kind: request_kind.to_owned(),
        native_thread_id: params.and_then(|params| string_path(params, &["threadId"])),
        native_turn_id: params.and_then(|params| string_path(params, &["turnId"])),
        native_item_id: params.and_then(|params| string_path(params, &["itemId"])),
        payload_redacted: include_redacted(options, request.params.as_ref()),
        native: native_server_request(request, options),
    })
}

fn map_codex_turn_started(
    notification: &CodexJsonlRpcNotificationEvent,
    options: RuntimeEventMappingOptions,
) -> RuntimeEvent {
    let Some(params) = notification.params.as_ref() else {
        return raw_notification(notification, options, "turn/started missing params");
    };
    let Some(native_turn_id) = string_path(params, &["turn", "id"]) else {
        return raw_notification(notification, options, "turn/started missing turn.id");
    };
    RuntimeEvent::TurnStarted(RuntimeTurnStarted {
        native_thread_id: string_path(params, &["threadId"]),
        native_turn_id,
        native: native_notification(notification, options),
    })
}

fn map_codex_turn_completed(
    notification: &CodexJsonlRpcNotificationEvent,
    options: RuntimeEventMappingOptions,
) -> RuntimeEvent {
    let Some(params) = notification.params.as_ref() else {
        return raw_notification(notification, options, "turn/completed missing params");
    };
    let Some(native_turn_id) = string_path(params, &["turn", "id"]) else {
        return raw_notification(notification, options, "turn/completed missing turn.id");
    };
    let status = string_path(params, &["turn", "status"]).unwrap_or_else(|| "completed".to_owned());
    if status == "failed" {
        return RuntimeEvent::TurnFailed(RuntimeTurnFailed {
            native_thread_id: string_path(params, &["threadId"]),
            native_turn_id: Some(native_turn_id),
            message: string_path(params, &["turn", "error", "message"])
                .unwrap_or_else(|| "Codex turn failed".to_owned()),
            code: string_path(params, &["turn", "error", "code"]),
            native: native_notification(notification, options),
        });
    }
    if status == "interrupted" || status == "cancelled" {
        return RuntimeEvent::TurnInterrupted(RuntimeTurnInterrupted {
            native_thread_id: string_path(params, &["threadId"]),
            native_turn_id,
            reason: status,
            native: native_notification(notification, options),
        });
    }
    RuntimeEvent::TurnCompleted(RuntimeTurnCompleted {
        native_thread_id: string_path(params, &["threadId"]),
        native_turn_id,
        status,
        native: native_notification(notification, options),
    })
}

fn map_codex_error(
    notification: &CodexJsonlRpcNotificationEvent,
    options: RuntimeEventMappingOptions,
) -> RuntimeEvent {
    let Some(params) = notification.params.as_ref() else {
        return raw_notification(notification, options, "error missing params");
    };
    let native_thread_id = string_path(params, &["threadId"]);
    let native_turn_id = string_path(params, &["turnId"]);
    let message = string_path(params, &["error", "message"])
        .or_else(|| string_path(params, &["message"]))
        .unwrap_or_else(|| "Codex runtime error".to_owned());
    let code = string_path(params, &["error", "code"]).or_else(|| string_path(params, &["code"]));
    let native = native_notification(notification, options);

    if bool_path(params, &["willRetry"]).unwrap_or(false) {
        return RuntimeEvent::TurnRetrying(RuntimeTurnRetrying {
            native_thread_id,
            native_turn_id,
            message,
            code,
            native,
        });
    }

    RuntimeEvent::Error(RuntimeErrorEvent {
        native_thread_id,
        native_turn_id,
        message,
        code,
        retryable: false,
        native,
    })
}

fn map_codex_item_started(
    notification: &CodexJsonlRpcNotificationEvent,
    options: RuntimeEventMappingOptions,
) -> RuntimeEvent {
    let Some(params) = notification.params.as_ref() else {
        return raw_notification(notification, options, "item/started missing params");
    };
    let Some(item) = params.get("item") else {
        return raw_notification(notification, options, "item/started missing item");
    };
    let Some(native_item_id) = string_path(item, &["id"]) else {
        return raw_notification(notification, options, "item/started missing item.id");
    };
    let item_kind = string_path(item, &["type"]).unwrap_or_else(|| "unknown".to_owned());
    if is_entered_review_mode_kind(item_kind.as_str()) {
        return map_codex_review_mode_item(notification, options, item, "entered");
    }
    if is_exited_review_mode_kind(item_kind.as_str()) {
        return map_codex_review_mode_item(notification, options, item, "exited");
    }
    let phase = codex_agent_message_phase(item_kind.as_str(), item);
    RuntimeEvent::ItemStarted(RuntimeItemStarted {
        native_thread_id: string_path(params, &["threadId"]),
        native_turn_id: string_path(params, &["turnId"]).unwrap_or_default(),
        native_item_id,
        item_kind,
        title: string_path(item, &["title"]),
        phase,
        metadata: codex_item_projection_metadata(item),
        native_item_redacted: include_redacted(options, Some(item)),
        native: native_notification(notification, options),
    })
}

fn map_codex_item_completed(
    notification: &CodexJsonlRpcNotificationEvent,
    options: RuntimeEventMappingOptions,
) -> RuntimeEvent {
    let Some(params) = notification.params.as_ref() else {
        return raw_notification(notification, options, "item/completed missing params");
    };
    let Some(item) = params.get("item") else {
        return raw_notification(notification, options, "item/completed missing item");
    };
    let Some(native_item_id) = string_path(item, &["id"]) else {
        return raw_notification(notification, options, "item/completed missing item.id");
    };
    let item_kind = string_path(item, &["type"]).unwrap_or_else(|| "unknown".to_owned());
    if is_entered_review_mode_kind(item_kind.as_str()) {
        return map_codex_review_mode_item(notification, options, item, "entered");
    }
    if is_exited_review_mode_kind(item_kind.as_str()) {
        return map_codex_review_mode_item(notification, options, item, "exited");
    }
    let phase = codex_agent_message_phase(item_kind.as_str(), item);
    RuntimeEvent::ItemCompleted(RuntimeItemCompleted {
        native_thread_id: string_path(params, &["threadId"]),
        native_turn_id: string_path(params, &["turnId"]).unwrap_or_default(),
        native_item_id,
        item_kind,
        text: item_text(item),
        summary: string_array_path(item, &["summary"]),
        content: string_array_path(item, &["content"]),
        phase,
        metadata: codex_item_projection_metadata(item),
        native_item_redacted: include_redacted(options, Some(item)),
        native: native_notification(notification, options),
    })
}

fn map_codex_item_delta(
    notification: &CodexJsonlRpcNotificationEvent,
    options: RuntimeEventMappingOptions,
    item_kind: &str,
    delta_kind: RuntimeItemDeltaKind,
) -> RuntimeEvent {
    let Some(params) = notification.params.as_ref() else {
        return raw_notification(notification, options, "item delta missing params");
    };
    let Some(native_item_id) = string_path(params, &["itemId"]) else {
        return raw_notification(notification, options, "item delta missing itemId");
    };
    RuntimeEvent::ItemDelta(RuntimeItemDelta {
        native_thread_id: string_path(params, &["threadId"]),
        native_turn_id: string_path(params, &["turnId"]).unwrap_or_default(),
        native_item_id,
        item_kind: item_kind.to_owned(),
        delta_kind,
        delta: string_path(params, &["delta"])
            .or_else(|| string_path(params, &["output"]))
            .or_else(|| string_path(params, &["text"]))
            .or_else(|| string_path(params, &["message"]))
            .unwrap_or_default(),
        metadata: codex_delta_projection_metadata(params),
        native: native_notification(notification, options),
    })
}

fn map_codex_reasoning_summary_part_added(
    notification: &CodexJsonlRpcNotificationEvent,
    options: RuntimeEventMappingOptions,
) -> RuntimeEvent {
    let Some(params) = notification.params.as_ref() else {
        return raw_notification(
            notification,
            options,
            "item/reasoning/summaryPartAdded missing params",
        );
    };
    let Some(native_item_id) = string_path(params, &["itemId"]) else {
        return raw_notification(
            notification,
            options,
            "item/reasoning/summaryPartAdded missing itemId",
        );
    };
    RuntimeEvent::ItemDelta(RuntimeItemDelta {
        native_thread_id: string_path(params, &["threadId"]),
        native_turn_id: string_path(params, &["turnId"]).unwrap_or_default(),
        native_item_id,
        item_kind: "reasoning".to_owned(),
        delta_kind: RuntimeItemDeltaKind::ReasoningSummary,
        delta: string_path(params, &["delta"])
            .or_else(|| string_path(params, &["text"]))
            .unwrap_or_else(|| "\n".to_owned()),
        metadata: codex_delta_projection_metadata(params),
        native: native_notification(notification, options),
    })
}

fn map_codex_plan_updated(
    notification: &CodexJsonlRpcNotificationEvent,
    options: RuntimeEventMappingOptions,
) -> RuntimeEvent {
    let Some(params) = notification.params.as_ref() else {
        return raw_notification(notification, options, "turn/plan/updated missing params");
    };
    RuntimeEvent::PlanUpdated(RuntimePlanUpdated {
        native_thread_id: string_path(params, &["threadId"]),
        native_turn_id: string_path(params, &["turnId"]).unwrap_or_default(),
        plan_redacted: params.get("plan").or_else(|| Some(params)).map(redact_json),
        native: native_notification(notification, options),
    })
}

fn map_codex_diff_updated(
    notification: &CodexJsonlRpcNotificationEvent,
    options: RuntimeEventMappingOptions,
) -> RuntimeEvent {
    let Some(params) = notification.params.as_ref() else {
        return raw_notification(notification, options, "turn/diff/updated missing params");
    };
    RuntimeEvent::DiffUpdated(RuntimeDiffUpdated {
        native_thread_id: string_path(params, &["threadId"]),
        native_turn_id: string_path(params, &["turnId"]).unwrap_or_default(),
        diff_redacted: params.get("diff").or_else(|| Some(params)).map(redact_json),
        native: native_notification(notification, options),
    })
}

fn map_codex_request_resolved(
    notification: &CodexJsonlRpcNotificationEvent,
    options: RuntimeEventMappingOptions,
) -> RuntimeEvent {
    let Some(params) = notification.params.as_ref() else {
        return raw_notification(
            notification,
            options,
            "serverRequest/resolved missing params",
        );
    };
    RuntimeEvent::RequestResolved(RuntimeRequestResolved {
        native_request_id: string_path(params, &["requestId"]).unwrap_or_default(),
        native: native_notification(notification, options),
    })
}

fn map_codex_review_mode_changed(
    notification: &CodexJsonlRpcNotificationEvent,
    options: RuntimeEventMappingOptions,
    fallback_status: &str,
) -> RuntimeEvent {
    let params = notification.params.as_ref();
    RuntimeEvent::ReviewModeChanged(RuntimeReviewModeChanged {
        native_thread_id: params.and_then(|params| {
            string_path(params, &["threadId"])
                .or_else(|| string_path(params, &["thread", "id"]))
                .or_else(|| string_path(params, &["reviewThreadId"]))
        }),
        native_turn_id: params.and_then(|params| {
            string_path(params, &["turnId"]).or_else(|| string_path(params, &["turn", "id"]))
        }),
        status: params
            .and_then(|params| string_path(params, &["status"]))
            .unwrap_or_else(|| fallback_status.to_owned()),
        message: params.and_then(|params| {
            string_path(params, &["message"]).or_else(|| string_path(params, &["reason"]))
        }),
        native: native_notification(notification, options),
    })
}

fn map_codex_review_mode_item(
    notification: &CodexJsonlRpcNotificationEvent,
    options: RuntimeEventMappingOptions,
    item: &JsonValue,
    fallback_status: &str,
) -> RuntimeEvent {
    let params = notification.params.as_ref();
    RuntimeEvent::ReviewModeChanged(RuntimeReviewModeChanged {
        native_thread_id: params.and_then(|params| {
            string_path(params, &["threadId"])
                .or_else(|| string_path(params, &["thread", "id"]))
                .or_else(|| string_path(item, &["threadId"]))
        }),
        native_turn_id: params.and_then(|params| {
            string_path(params, &["turnId"])
                .or_else(|| string_path(params, &["turn", "id"]))
                .or_else(|| string_path(item, &["turnId"]))
        }),
        status: string_path(item, &["status"]).unwrap_or_else(|| fallback_status.to_owned()),
        message: string_path(item, &["review"])
            .or_else(|| item_text(item))
            .or_else(|| string_path(item, &["message"])),
        native: native_notification(notification, options),
    })
}

fn is_entered_review_mode_kind(kind: &str) -> bool {
    normalize_event_kind(kind) == "enteredreviewmode"
}

fn is_exited_review_mode_kind(kind: &str) -> bool {
    normalize_event_kind(kind) == "exitedreviewmode"
}

fn normalize_event_kind(kind: &str) -> String {
    kind.chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

fn raw_notification(
    notification: &CodexJsonlRpcNotificationEvent,
    options: RuntimeEventMappingOptions,
    reason: &str,
) -> RuntimeEvent {
    let params = notification.params.as_ref();
    RuntimeEvent::Raw(RuntimeRawEvent {
        native_method: notification.method.clone(),
        reason: reason.to_owned(),
        native_thread_id: params.and_then(|params| {
            string_path(params, &["threadId"]).or_else(|| string_path(params, &["thread", "id"]))
        }),
        native_turn_id: params.and_then(|params| {
            string_path(params, &["turnId"]).or_else(|| string_path(params, &["turn", "id"]))
        }),
        native_item_id: params.and_then(|params| {
            string_path(params, &["itemId"]).or_else(|| string_path(params, &["item", "id"]))
        }),
        payload_redacted: include_redacted(options, notification.params.as_ref()),
        raw_redacted: include_redacted(options, Some(&notification.raw)),
    })
}

fn raw_notification_with_payload(
    notification: &CodexJsonlRpcNotificationEvent,
    options: RuntimeEventMappingOptions,
    reason: &str,
) -> RuntimeEvent {
    let params = notification.params.as_ref();
    RuntimeEvent::Raw(RuntimeRawEvent {
        native_method: notification.method.clone(),
        reason: reason.to_owned(),
        native_thread_id: params.and_then(|params| {
            string_path(params, &["threadId"]).or_else(|| string_path(params, &["thread", "id"]))
        }),
        native_turn_id: params.and_then(|params| {
            string_path(params, &["turnId"]).or_else(|| string_path(params, &["turn", "id"]))
        }),
        native_item_id: params.and_then(|params| {
            string_path(params, &["itemId"]).or_else(|| string_path(params, &["item", "id"]))
        }),
        payload_redacted: notification.params.as_ref().map(redact_json),
        raw_redacted: include_redacted(options, Some(&notification.raw)),
    })
}

fn native_notification(
    notification: &CodexJsonlRpcNotificationEvent,
    options: RuntimeEventMappingOptions,
) -> Option<RuntimeNativeEvent> {
    options
        .include_redacted_native_payload
        .then(|| RuntimeNativeEvent {
            method: notification.method.clone(),
            payload_redacted: notification.params.as_ref().map(redact_json),
            raw_redacted: Some(redact_json(&notification.raw)),
        })
}

fn native_server_request(
    request: &CodexJsonlRpcServerRequest,
    options: RuntimeEventMappingOptions,
) -> Option<RuntimeNativeEvent> {
    options
        .include_redacted_native_payload
        .then(|| RuntimeNativeEvent {
            method: request.method.clone(),
            payload_redacted: request.params.as_ref().map(redact_json),
            raw_redacted: Some(redact_json(&request.raw)),
        })
}

fn include_redacted(
    options: RuntimeEventMappingOptions,
    value: Option<&JsonValue>,
) -> Option<JsonValue> {
    options
        .include_redacted_native_payload
        .then(|| value.map(redact_json))
        .flatten()
}

fn command_delta_kind(params: Option<&JsonValue>) -> RuntimeItemDeltaKind {
    match params.and_then(|params| string_path(params, &["stream"])) {
        Some(stream) if stream.eq_ignore_ascii_case("stderr") => RuntimeItemDeltaKind::Stderr,
        Some(stream) if stream.eq_ignore_ascii_case("stdout") => RuntimeItemDeltaKind::Stdout,
        _ => RuntimeItemDeltaKind::Stdout,
    }
}

fn codex_item_projection_metadata(item: &JsonValue) -> Option<JsonValue> {
    let mut metadata = JsonMap::new();

    insert_string_array(
        &mut metadata,
        "command",
        first_non_empty_string_array(item, &[&["command"], &["cmd"], &["argv"]]),
    );
    insert_string(
        &mut metadata,
        "cwd",
        first_string_path(item, &[&["cwd"], &["workingDirectory"]]),
    );
    insert_string_array(
        &mut metadata,
        "changedFiles",
        changed_files_from_value(item),
    );
    insert_i64(
        &mut metadata,
        "exitCode",
        first_i64_path(item, &[&["exitCode"], &["exit_code"]]),
    );
    insert_string(
        &mut metadata,
        "status",
        first_string_path(item, &[&["status"], &["state"]]),
    );
    insert_bool(
        &mut metadata,
        "success",
        first_bool_path(item, &[&["success"]]),
    );
    insert_string(
        &mut metadata,
        "stdout",
        first_string_path(item, &[&["stdout"]]),
    );
    insert_string(
        &mut metadata,
        "stderr",
        first_string_path(item, &[&["stderr"]]),
    );
    insert_string(
        &mut metadata,
        "operation",
        first_string_path(item, &[&["operation"]]),
    );
    insert_string(
        &mut metadata,
        "toolName",
        first_string_path(item, &[&["toolName"], &["tool_name"], &["tool"]]),
    );
    insert_string(
        &mut metadata,
        "namespace",
        first_string_path(
            item,
            &[&["namespace"], &["toolNamespace"], &["tool_namespace"]],
        ),
    );
    insert_string(
        &mut metadata,
        "message",
        first_string_path(item, &[&["message"], &["text"], &["review"]]),
    );
    insert_string(
        &mut metadata,
        "query",
        first_string_path(item, &[&["query"], &["action", "query"]]),
    );
    insert_string_array(
        &mut metadata,
        "queries",
        first_non_empty_string_array(item, &[&["queries"], &["action", "queries"]]),
    );
    insert_string(
        &mut metadata,
        "url",
        first_string_path(item, &[&["url"], &["action", "url"]]),
    );
    insert_string(
        &mut metadata,
        "pattern",
        first_string_path(item, &[&["pattern"], &["action", "pattern"]]),
    );
    insert_string(
        &mut metadata,
        "provider",
        first_string_path(item, &[&["provider"]]),
    );
    insert_string(&mut metadata, "path", first_string_path(item, &[&["path"]]));
    insert_string(
        &mut metadata,
        "server",
        first_string_path(item, &[&["server"], &["serverName"]]),
    );
    insert_string(
        &mut metadata,
        "tool",
        first_string_path(item, &[&["tool"], &["toolName"], &["tool_name"]]),
    );
    insert_string(
        &mut metadata,
        "senderThreadId",
        first_string_path(item, &[&["senderThreadId"]]),
    );
    insert_string(
        &mut metadata,
        "receiverThreadId",
        first_string_path(item, &[&["receiverThreadId"]]),
    );
    insert_string(
        &mut metadata,
        "newThreadId",
        first_string_path(item, &[&["newThreadId"]]),
    );
    insert_string(
        &mut metadata,
        "prompt",
        first_string_path(item, &[&["prompt"]]),
    );
    insert_string(
        &mut metadata,
        "agentStatus",
        first_string_path(item, &[&["agentStatus"]]),
    );
    insert_i64(
        &mut metadata,
        "compressedTokens",
        first_i64_path(
            item,
            &[
                &["compressedTokens"],
                &["compressed_tokens"],
                &["tokens"],
                &["tokenCount"],
            ],
        ),
    );
    insert_i64(
        &mut metadata,
        "durationMs",
        first_i64_path(item, &[&["durationMs"], &["duration_ms"], &["tookMs"]]),
    );
    insert_i64(
        &mut metadata,
        "resultCount",
        first_i64_path(item, &[&["resultCount"], &["result_count"]]),
    );
    insert_json(
        &mut metadata,
        "patch",
        first_json_path(item, &[&["patch"], &["diff"]]),
    );
    insert_json(
        &mut metadata,
        "action",
        first_json_path(item, &[&["action"]]),
    );
    insert_json(
        &mut metadata,
        "arguments",
        first_json_path(item, &[&["arguments"], &["args"]]),
    );
    insert_json(
        &mut metadata,
        "result",
        first_json_path(item, &[&["result"]]),
    );
    insert_json(&mut metadata, "error", first_json_path(item, &[&["error"]]));
    insert_json(
        &mut metadata,
        "contentItems",
        first_json_path(item, &[&["contentItems"], &["content_items"]]),
    );

    (!metadata.is_empty()).then(|| JsonValue::Object(metadata))
}

fn codex_delta_projection_metadata(params: &JsonValue) -> Option<JsonValue> {
    let mut metadata = JsonMap::new();

    insert_string(
        &mut metadata,
        "stream",
        first_string_path(params, &[&["stream"]]),
    );
    insert_string_array(
        &mut metadata,
        "changedFiles",
        changed_files_from_value(params),
    );
    insert_string(
        &mut metadata,
        "status",
        first_string_path(params, &[&["status"], &["state"]]),
    );
    insert_i64(
        &mut metadata,
        "summaryIndex",
        first_i64_path(params, &[&["summaryIndex"], &["summary_index"]]),
    );
    insert_json(
        &mut metadata,
        "patch",
        first_json_path(params, &[&["patch"], &["diff"]]),
    );

    (!metadata.is_empty()).then(|| JsonValue::Object(metadata))
}

fn item_text(item: &JsonValue) -> Option<String> {
    string_path(item, &["text"])
        .or_else(|| string_path(item, &["message"]))
        .or_else(|| string_path(item, &["content", "text"]))
        .or_else(|| {
            let content = string_array_path(item, &["content"]);
            (!content.is_empty()).then(|| content.join(""))
        })
}

fn first_string_path(value: &JsonValue, paths: &[&[&str]]) -> Option<String> {
    paths.iter().find_map(|path| string_path(value, path))
}

fn first_bool_path(value: &JsonValue, paths: &[&[&str]]) -> Option<bool> {
    paths.iter().find_map(|path| bool_path(value, path))
}

fn first_i64_path(value: &JsonValue, paths: &[&[&str]]) -> Option<i64> {
    paths.iter().find_map(|path| i64_path(value, path))
}

fn first_json_path(value: &JsonValue, paths: &[&[&str]]) -> Option<JsonValue> {
    paths
        .iter()
        .find_map(|path| json_path(value, path).cloned())
}

fn first_non_empty_string_array(value: &JsonValue, paths: &[&[&str]]) -> Vec<String> {
    paths
        .iter()
        .map(|path| string_array_path(value, path))
        .find(|values| !values.is_empty())
        .unwrap_or_default()
}

fn changed_files_from_value(value: &JsonValue) -> Vec<String> {
    let changed_files = first_non_empty_string_array(
        value,
        &[
            &["changedFiles"],
            &["changed_files"],
            &["files"],
            &["paths"],
            &["changed", "files"],
        ],
    );
    if !changed_files.is_empty() {
        return changed_files;
    }
    string_path(value, &["path"]).into_iter().collect()
}

fn insert_string(metadata: &mut JsonMap<String, JsonValue>, key: &str, value: Option<String>) {
    if let Some(value) = value
        && !value.is_empty()
    {
        metadata.insert(key.to_owned(), JsonValue::String(value));
    }
}

fn insert_bool(metadata: &mut JsonMap<String, JsonValue>, key: &str, value: Option<bool>) {
    if let Some(value) = value {
        metadata.insert(key.to_owned(), JsonValue::Bool(value));
    }
}

fn insert_i64(metadata: &mut JsonMap<String, JsonValue>, key: &str, value: Option<i64>) {
    if let Some(value) = value {
        metadata.insert(key.to_owned(), JsonValue::Number(value.into()));
    }
}

fn insert_string_array(metadata: &mut JsonMap<String, JsonValue>, key: &str, values: Vec<String>) {
    if !values.is_empty() {
        metadata.insert(
            key.to_owned(),
            JsonValue::Array(values.into_iter().map(JsonValue::String).collect()),
        );
    }
}

fn insert_json(metadata: &mut JsonMap<String, JsonValue>, key: &str, value: Option<JsonValue>) {
    if let Some(value) = value
        && !value.is_null()
    {
        metadata.insert(key.to_owned(), value);
    }
}

fn string_path(value: &JsonValue, path: &[&str]) -> Option<String> {
    let mut current = value;
    for segment in path {
        current = current.get(*segment)?;
    }
    match current {
        JsonValue::String(value) => Some(value.clone()),
        JsonValue::Number(value) => Some(value.to_string()),
        JsonValue::Bool(value) => Some(value.to_string()),
        JsonValue::Null | JsonValue::Array(_) | JsonValue::Object(_) => None,
    }
}

fn bool_path(value: &JsonValue, path: &[&str]) -> Option<bool> {
    let mut current = value;
    for segment in path {
        current = current.get(*segment)?;
    }
    current.as_bool()
}

fn i64_path(value: &JsonValue, path: &[&str]) -> Option<i64> {
    let mut current = value;
    for segment in path {
        current = current.get(*segment)?;
    }
    match current {
        JsonValue::Number(value) => value
            .as_i64()
            .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok())),
        JsonValue::String(value) => value.parse::<i64>().ok(),
        _ => None,
    }
}

fn json_path<'a>(value: &'a JsonValue, path: &[&str]) -> Option<&'a JsonValue> {
    let mut current = value;
    for segment in path {
        current = current.get(*segment)?;
    }
    Some(current)
}

fn string_array_path(value: &JsonValue, path: &[&str]) -> Vec<String> {
    let mut current = value;
    for segment in path {
        let Some(next) = current.get(*segment) else {
            return Vec::new();
        };
        current = next;
    }
    match current {
        JsonValue::Array(values) => values
            .iter()
            .filter_map(|value| match value {
                JsonValue::String(value) => Some(value.clone()),
                JsonValue::Object(_) => item_text(value),
                _ => None,
            })
            .collect(),
        JsonValue::String(value) => vec![value.clone()],
        _ => Vec::new(),
    }
}

fn codex_agent_message_phase(item_kind: &str, item: &JsonValue) -> RuntimeAgentMessagePhase {
    if !matches!(
        normalize_kind(item_kind).as_str(),
        "agentmessage" | "assistantmessage" | "message"
    ) {
        return RuntimeAgentMessagePhase::FinalAnswer;
    }

    let phase = first_string_path(item, &[&["phase"]]).map(|phase| normalize_phase(&phase));
    match phase.as_deref() {
        Some("commentary") => RuntimeAgentMessagePhase::Commentary,
        _ => RuntimeAgentMessagePhase::FinalAnswer,
    }
}

fn normalize_kind(kind: &str) -> String {
    kind.chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

fn normalize_phase(phase: &str) -> String {
    phase
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

fn redact_json(value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::Object(map) => {
            let redacted = map
                .iter()
                .map(|(key, value)| {
                    if is_secret_key(key) {
                        (key.clone(), JsonValue::String("[REDACTED]".to_owned()))
                    } else {
                        (key.clone(), redact_json(value))
                    }
                })
                .collect();
            JsonValue::Object(redacted)
        }
        JsonValue::Array(values) => JsonValue::Array(values.iter().map(redact_json).collect()),
        value => value.clone(),
    }
}

fn is_secret_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    key.contains("token")
        || key.contains("secret")
        || key.contains("password")
        || key.contains("authorization")
        || key.contains("apikey")
        || key.contains("api_key")
}

#[cfg(test)]
mod tests {
    use super::{
        RuntimeAgentMessagePhase, RuntimeEvent, RuntimeEventMappingOptions, RuntimeItemDeltaKind,
        RuntimeTurnTerminalKind, map_codex_notification_event, map_codex_server_request_event,
    };
    use crate::codex::{CodexJsonlRpcNotificationEvent, CodexJsonlRpcServerRequest};
    use crate::driver::JsonlRpcId;
    use serde_json::json;

    #[test]
    fn event_codex_unknown_notification_maps_to_raw_without_crashing() {
        let event = map_codex_notification_event(
            &CodexJsonlRpcNotificationEvent {
                method: "future/event".to_owned(),
                params: Some(json!({
                    "threadId": "native_thread_1",
                    "apiToken": "secret"
                })),
                raw: json!({"method": "future/event"}),
            },
            RuntimeEventMappingOptions {
                include_redacted_native_payload: true,
            },
        );

        let RuntimeEvent::Raw(raw) = event else {
            panic!("expected raw event");
        };
        assert_eq!(raw.native_method, "future/event");
        assert_eq!(raw.native_thread_id.as_deref(), Some("native_thread_1"));
        assert_eq!(
            raw.payload_redacted
                .as_ref()
                .and_then(|payload| payload.get("apiToken")),
            Some(&json!("[REDACTED]"))
        );
    }

    #[test]
    fn event_diagnostics_redacts_native_tokens_and_headers() {
        let event = map_codex_notification_event(
            &CodexJsonlRpcNotificationEvent {
                method: "future/diagnostics".to_owned(),
                params: Some(json!({
                    "headers": {
                        "Authorization": "Bearer sk-secret"
                    },
                    "apiToken": "secret",
                    "nested": {
                        "refresh_token": "refresh-secret"
                    }
                })),
                raw: json!({
                    "method": "future/diagnostics",
                    "params": {
                        "authorization": "Bearer sk-secret",
                        "apiKey": "secret"
                    }
                }),
            },
            RuntimeEventMappingOptions {
                include_redacted_native_payload: true,
            },
        );

        let RuntimeEvent::Raw(raw) = event else {
            panic!("expected raw event");
        };
        let payload = raw.payload_redacted.as_ref().expect("redacted payload");
        assert_eq!(payload["headers"]["Authorization"], json!("[REDACTED]"));
        assert_eq!(payload["apiToken"], json!("[REDACTED]"));
        assert_eq!(payload["nested"]["refresh_token"], json!("[REDACTED]"));
        let raw = raw.raw_redacted.as_ref().expect("redacted raw");
        assert_eq!(raw["params"]["authorization"], json!("[REDACTED]"));
        assert_eq!(raw["params"]["apiKey"], json!("[REDACTED]"));
    }

    #[test]
    fn event_codex_turn_started_maps_to_canonical_turn_event() {
        let event = map_codex_notification_event(
            &CodexJsonlRpcNotificationEvent {
                method: "turn/started".to_owned(),
                params: Some(json!({
                    "threadId": "native_thread_1",
                    "turn": {"id": "native_turn_1"}
                })),
                raw: json!({"method": "turn/started"}),
            },
            RuntimeEventMappingOptions::default(),
        );

        let RuntimeEvent::TurnStarted(turn) = event else {
            panic!("expected turn started");
        };
        assert_eq!(turn.native_thread_id.as_deref(), Some("native_thread_1"));
        assert_eq!(turn.native_turn_id, "native_turn_1");
        assert!(turn.native.is_none());
    }

    #[test]
    fn event_codex_review_mode_notification_maps_to_review_event() {
        let event = map_codex_notification_event(
            &CodexJsonlRpcNotificationEvent {
                method: "enteredReviewMode".to_owned(),
                params: Some(json!({
                    "threadId": "native_thread_1",
                    "turnId": "native_turn_review_1",
                    "message": "Reviewing current diff"
                })),
                raw: json!({"method": "enteredReviewMode"}),
            },
            RuntimeEventMappingOptions::default(),
        );

        let RuntimeEvent::ReviewModeChanged(review) = event else {
            panic!("expected review mode changed");
        };
        assert_eq!(review.native_thread_id.as_deref(), Some("native_thread_1"));
        assert_eq!(
            review.native_turn_id.as_deref(),
            Some("native_turn_review_1")
        );
        assert_eq!(review.status, "entered");
        assert_eq!(review.message.as_deref(), Some("Reviewing current diff"));
    }

    #[test]
    fn event_codex_review_mode_item_maps_to_review_event() {
        let entered = map_codex_notification_event(
            &CodexJsonlRpcNotificationEvent {
                method: "item/started".to_owned(),
                params: Some(json!({
                    "threadId": "native_thread_1",
                    "turnId": "native_turn_review_1",
                    "item": {
                        "id": "native_review_entered_1",
                        "type": "enteredReviewMode",
                        "review": "Reviewing current diff"
                    }
                })),
                raw: json!({"method": "item/started"}),
            },
            RuntimeEventMappingOptions::default(),
        );
        let exited = map_codex_notification_event(
            &CodexJsonlRpcNotificationEvent {
                method: "item/completed".to_owned(),
                params: Some(json!({
                    "threadId": "native_thread_1",
                    "turnId": "native_turn_review_1",
                    "item": {
                        "id": "native_review_exited_1",
                        "type": "exitedReviewMode",
                        "status": "completed"
                    }
                })),
                raw: json!({"method": "item/completed"}),
            },
            RuntimeEventMappingOptions::default(),
        );

        let RuntimeEvent::ReviewModeChanged(entered) = entered else {
            panic!("expected entered review mode changed");
        };
        assert_eq!(entered.native_thread_id.as_deref(), Some("native_thread_1"));
        assert_eq!(
            entered.native_turn_id.as_deref(),
            Some("native_turn_review_1")
        );
        assert_eq!(entered.status, "entered");
        assert_eq!(entered.message.as_deref(), Some("Reviewing current diff"));

        let RuntimeEvent::ReviewModeChanged(exited) = exited else {
            panic!("expected exited review mode changed");
        };
        assert_eq!(exited.native_thread_id.as_deref(), Some("native_thread_1"));
        assert_eq!(
            exited.native_turn_id.as_deref(),
            Some("native_turn_review_1")
        );
        assert_eq!(exited.status, "completed");
    }

    #[test]
    fn event_codex_turn_completed_failed_maps_to_turn_failed() {
        let event = map_codex_notification_event(
            &CodexJsonlRpcNotificationEvent {
                method: "turn/completed".to_owned(),
                params: Some(json!({
                    "threadId": "native_thread_1",
                    "turn": {
                        "id": "native_turn_1",
                        "status": "failed",
                        "error": {"message": "boom", "code": "failed"}
                    }
                })),
                raw: json!({"method": "turn/completed"}),
            },
            RuntimeEventMappingOptions::default(),
        );

        let RuntimeEvent::TurnFailed(turn) = event else {
            panic!("expected turn failed");
        };
        assert_eq!(turn.native_turn_id.as_deref(), Some("native_turn_1"));
        assert_eq!(turn.message, "boom");
        assert_eq!(turn.code.as_deref(), Some("failed"));
    }

    #[test]
    fn event_codex_retryable_error_maps_to_turn_retrying() {
        let event = map_codex_notification_event(
            &CodexJsonlRpcNotificationEvent {
                method: "error".to_owned(),
                params: Some(json!({
                    "threadId": "native_thread_1",
                    "turnId": "native_turn_1",
                    "error": {
                        "message": "Reconnecting... 2/5",
                        "code": "stream_disconnected"
                    },
                    "willRetry": true
                })),
                raw: json!({"method": "error"}),
            },
            RuntimeEventMappingOptions::default(),
        );

        let RuntimeEvent::TurnRetrying(retrying) = event else {
            panic!("expected turn retrying");
        };
        assert_eq!(
            retrying.native_thread_id.as_deref(),
            Some("native_thread_1")
        );
        assert_eq!(retrying.native_turn_id.as_deref(), Some("native_turn_1"));
        assert_eq!(retrying.message, "Reconnecting... 2/5");
        assert_eq!(retrying.code.as_deref(), Some("stream_disconnected"));
    }

    #[test]
    fn event_codex_non_retryable_error_remains_terminal_runtime_error() {
        let event = map_codex_notification_event(
            &CodexJsonlRpcNotificationEvent {
                method: "error".to_owned(),
                params: Some(json!({
                    "threadId": "native_thread_1",
                    "turnId": "native_turn_1",
                    "error": {"message": "request failed"},
                    "willRetry": false
                })),
                raw: json!({"method": "error"}),
            },
            RuntimeEventMappingOptions::default(),
        );

        assert_eq!(
            event.turn_terminal_kind(),
            Some(RuntimeTurnTerminalKind::Failed)
        );
        let RuntimeEvent::Error(error) = event else {
            panic!("expected runtime error");
        };
        assert!(!error.retryable);
        assert_eq!(error.message, "request failed");
    }

    #[test]
    fn event_codex_unscoped_error_is_not_turn_terminal() {
        let event = map_codex_notification_event(
            &CodexJsonlRpcNotificationEvent {
                method: "error".to_owned(),
                params: Some(json!({
                    "error": {"message": "session diagnostic"},
                    "willRetry": false
                })),
                raw: json!({"method": "error"}),
            },
            RuntimeEventMappingOptions::default(),
        );

        assert_eq!(event.turn_terminal_kind(), None);
    }

    #[test]
    fn event_codex_item_agent_delta_maps_to_canonical_delta() {
        let event = map_codex_notification_event(
            &CodexJsonlRpcNotificationEvent {
                method: "item/agentMessage/delta".to_owned(),
                params: Some(json!({
                    "threadId": "native_thread_1",
                    "turnId": "native_turn_1",
                    "itemId": "native_item_1",
                    "delta": "hello"
                })),
                raw: json!({"method": "item/agentMessage/delta"}),
            },
            RuntimeEventMappingOptions::default(),
        );

        let RuntimeEvent::ItemDelta(delta) = event else {
            panic!("expected item delta");
        };
        assert_eq!(delta.native_turn_id, "native_turn_1");
        assert_eq!(delta.native_item_id, "native_item_1");
        assert_eq!(delta.item_kind, "agentMessage");
        assert_eq!(delta.delta_kind, RuntimeItemDeltaKind::AgentMessage);
        assert_eq!(delta.delta, "hello");
    }

    #[test]
    fn event_codex_agent_message_phase_maps_to_runtime_attribute() {
        let started = map_codex_notification_event(
            &CodexJsonlRpcNotificationEvent {
                method: "item/started".to_owned(),
                params: Some(json!({
                    "threadId": "native_thread_1",
                    "turnId": "native_turn_1",
                    "item": {
                        "id": "native_item_1",
                        "type": "agentMessage",
                        "phase": "commentary"
                    }
                })),
                raw: json!({"method": "item/started"}),
            },
            RuntimeEventMappingOptions::default(),
        );
        let completed = map_codex_notification_event(
            &CodexJsonlRpcNotificationEvent {
                method: "item/completed".to_owned(),
                params: Some(json!({
                    "threadId": "native_thread_1",
                    "turnId": "native_turn_1",
                    "item": {
                        "id": "native_item_1",
                        "type": "agentMessage",
                        "phase": "commentary",
                        "text": "I will inspect the project."
                    }
                })),
                raw: json!({"method": "item/completed"}),
            },
            RuntimeEventMappingOptions::default(),
        );

        let RuntimeEvent::ItemStarted(started) = started else {
            panic!("expected started item");
        };
        assert_eq!(started.phase, RuntimeAgentMessagePhase::Commentary);
        assert!(
            started
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get("phase"))
                .is_none(),
            "agent message phase must not be tunneled through metadata"
        );

        let RuntimeEvent::ItemCompleted(completed) = completed else {
            panic!("expected completed item");
        };
        assert_eq!(completed.phase, RuntimeAgentMessagePhase::Commentary);
        assert!(
            completed
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get("phase"))
                .is_none(),
            "agent message phase must not be tunneled through metadata"
        );
    }

    #[test]
    fn event_codex_server_request_maps_to_request_opened() {
        let event = map_codex_server_request_event(
            &CodexJsonlRpcServerRequest {
                id: JsonlRpcId::Number(7),
                method: "item/commandExecution/requestApproval".to_owned(),
                params: Some(json!({
                    "threadId": "native_thread_1",
                    "turnId": "native_turn_1",
                    "itemId": "native_item_1"
                })),
                raw: json!({"id": 7, "method": "item/commandExecution/requestApproval"}),
            },
            RuntimeEventMappingOptions::default(),
        );

        let RuntimeEvent::RequestOpened(request) = event else {
            panic!("expected request opened");
        };
        assert_eq!(request.native_request_id, "7");
        assert_eq!(request.native_request_id_json, Some(json!(7)));
        assert_eq!(request.request_kind, "command_approval");
        assert_eq!(request.native_item_id.as_deref(), Some("native_item_1"));
    }

    #[test]
    fn codex_mcp_event_maps_native_lifecycle_progress_and_permission_callback() {
        let started = map_codex_notification_event(
            &CodexJsonlRpcNotificationEvent {
                method: "item/started".to_owned(),
                params: Some(json!({
                    "threadId": "native_thread_1",
                    "turnId": "native_turn_1",
                    "item": {
                        "id": "native_mcp_1",
                        "type": "mcpToolCall",
                        "server": "pioneer",
                        "tool": "mcp__server__tool",
                        "arguments": {"value": 7},
                        "status": "inProgress"
                    }
                })),
                raw: json!({"method": "item/started"}),
            },
            RuntimeEventMappingOptions::default(),
        );
        let progress = map_codex_notification_event(
            &CodexJsonlRpcNotificationEvent {
                method: "item/mcpToolCall/progress".to_owned(),
                params: Some(json!({
                    "threadId": "native_thread_1",
                    "turnId": "native_turn_1",
                    "itemId": "native_mcp_1",
                    "message": "working"
                })),
                raw: json!({"method": "item/mcpToolCall/progress"}),
            },
            RuntimeEventMappingOptions::default(),
        );
        let completed = map_codex_notification_event(
            &CodexJsonlRpcNotificationEvent {
                method: "item/completed".to_owned(),
                params: Some(json!({
                    "threadId": "native_thread_1",
                    "turnId": "native_turn_1",
                    "item": {
                        "id": "native_mcp_1",
                        "type": "mcpToolCall",
                        "server": "pioneer",
                        "tool": "mcp__server__tool",
                        "arguments": {"value": 7},
                        "status": "failed",
                        "error": {"message": "upstream failed"}
                    }
                })),
                raw: json!({"method": "item/completed"}),
            },
            RuntimeEventMappingOptions::default(),
        );
        let permission = map_codex_server_request_event(
            &CodexJsonlRpcServerRequest {
                id: JsonlRpcId::Number(11),
                method: "item/permissions/requestApproval".to_owned(),
                params: Some(json!({
                    "threadId": "native_thread_1",
                    "turnId": "native_turn_1",
                    "itemId": "native_mcp_1",
                    "permissions": {"network": ["example.com"]},
                    "cwd": "/workspace",
                    "startedAtMs": 1
                })),
                raw: json!({"id": 11, "method": "item/permissions/requestApproval"}),
            },
            RuntimeEventMappingOptions::default(),
        );

        let RuntimeEvent::ItemStarted(started) = started else {
            panic!("expected MCP item start");
        };
        assert_eq!(started.item_kind, "mcpToolCall");
        assert_eq!(started.native_item_id, "native_mcp_1");
        assert_eq!(
            started
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get("server")),
            Some(&json!("pioneer"))
        );
        assert_eq!(
            started
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get("arguments")),
            Some(&json!({"value": 7}))
        );

        let RuntimeEvent::ItemDelta(progress) = progress else {
            panic!("expected MCP progress delta");
        };
        assert_eq!(progress.item_kind, "mcpToolCall");
        assert_eq!(progress.delta_kind, RuntimeItemDeltaKind::ToolProgress);
        assert_eq!(progress.native_item_id, "native_mcp_1");
        assert_eq!(progress.delta, "working");

        let RuntimeEvent::ItemCompleted(completed) = completed else {
            panic!("expected MCP item completion");
        };
        assert_eq!(completed.item_kind, "mcpToolCall");
        assert_eq!(completed.native_item_id, "native_mcp_1");
        assert_eq!(
            completed
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get("status")),
            Some(&json!("failed"))
        );

        let RuntimeEvent::RequestOpened(permission) = permission else {
            panic!("expected MCP permission request");
        };
        assert_eq!(permission.request_kind, "permission_approval");
        assert_eq!(permission.native_item_id.as_deref(), Some("native_mcp_1"));
        assert_eq!(permission.native_request_id_json, Some(json!(11)));
    }
}
