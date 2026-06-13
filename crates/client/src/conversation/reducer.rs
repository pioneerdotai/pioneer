use super::state_machine::TurnStateMachine;
use pioneer_protocol::{
    ExecutionWindowExhaustionReason, ExecutionWindowStatus, MarkdownDocument, RecoveryJobStatus,
    SystemEventLevel, TimelineOrigin, ToolLoopBudgetAction, ToolLoopBudgetLimitKind,
    ToolRetryBudgetUsage, ToolRetryErrorClass, ToolRetryExhaustionKind, ToolRetryResolution, Turn,
    TurnBlockedResumeMetadata, TurnExecutionWindowBlockedNotification,
    TurnExecutionWindowCheckpointedNotification, TurnExecutionWindowContinuedNotification,
    TurnExecutionWindowExhaustedNotification, TurnExecutionWindowStartedNotification, TurnItem,
    TurnItemTimeoutReason, TurnItemType, TurnStatus, UserMessageAttachment,
};
use serde_json::Value as JsonValue;
use std::{
    cmp::Ordering,
    collections::{HashMap, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
};

macro_rules! client_system_label {
    ("timeline.system.timeout_delta_recovery_scheduled", attempt = $attempt:expr, reason = $reason:expr, job_id = $job_id:expr) => {
        format!(
            "[timeout] attempt #{} {}; recovery scheduled (job `{}`)",
            $attempt, $reason, $job_id
        )
    };
    ("timeline.system.timeout_delta_no_recovery", attempt = $attempt:expr, reason = $reason:expr) => {
        format!(
            "[timeout] attempt #{} {}; no automatic recovery",
            $attempt, $reason
        )
    };
    ("timeline.system.timeout_with_recovery") => {
        "The step timed out; recovery started".to_owned()
    };
    ("timeline.system.timeout_without_recovery") => {
        "The step timed out; automatic recovery is unavailable".to_owned()
    };
    ("timeline.system.recovery_opened") => {
        "Starting step recovery".to_owned()
    };
    ("timeline.system.recovery_attached") => {
        "Recovery attached to a new step".to_owned()
    };
    ("timeline.system.retry_scheduled") => {
        "Recovery retry scheduled".to_owned()
    };
    ("timeline.system.retry_started") => {
        "Recovery retry started".to_owned()
    };
    ("timeline.system.recovery_succeeded") => {
        "Recovery completed".to_owned()
    };
    ("timeline.system.recovery_failed") => {
        "Recovery failed".to_owned()
    };
    ("timeline.system.tool_retry_scheduled_with_attempt", tool_name = $tool_name:expr, attempt = $attempt:expr) => {
        format!("Retrying tool {} (attempt #{})", $tool_name, $attempt)
    };
    ("timeline.system.tool_retry_resolved", tool_name = $tool_name:expr) => {
        format!("{} completed after retry", $tool_name)
    };
    ("timeline.system.tool_retry_exhausted", tool_name = $tool_name:expr) => {
        format!("{} failed after retries", $tool_name)
    };
    ("timeline.system.tool_loop_budget_exceeded") => {
        "Tool call limit reached".to_owned()
    };
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub enum TimelineEntryStatus {
    Running,
    Completed,
    Blocked,
    Failed,
    Cancelled,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum TurnPhase {
    Starting,
    Running,
    Completing,
    Completed,
    Blocked,
    Failed,
    Cancelled,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct TurnView {
    pub id: String,
    pub phase: TurnPhase,
    pub started_at_unix_ms: Option<i64>,
    pub completed_at_unix_ms: Option<i64>,
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume: Option<TurnBlockedResumeMetadata>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct ItemView {
    pub id: String,
    pub turn_id: String,
    pub item_type: String,
    pub status: TimelineEntryStatus,
    pub started_at_unix_ms: Option<i64>,
    pub updated_at_unix_ms: Option<i64>,
    pub completed_at_unix_ms: Option<i64>,
    pub partial_text: String,
    pub final_text: Option<String>,
    pub partial_markdown: Option<MarkdownDocument>,
    pub final_markdown: Option<MarkdownDocument>,
    pub item: TurnItem,
    pub timeline_origin: Option<TimelineOrigin>,
    pub opaque_meta: Option<JsonValue>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct TimelineEntry {
    pub id: String,
    pub turn_id: String,
    pub item_id: String,
    pub item_index: usize,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ConversationViewState {
    pub timeline: Vec<TimelineEntry>,
    pub turns: Vec<TurnView>,
    pub items: Vec<ItemView>,
    pub revision: u64,
    pub composer_locked: bool,
    pub in_flight_turn_id: Option<String>,
    pub pending_request_id: Option<String>,
    pub phase_label: String,
    pub last_error: Option<String>,
}

impl ConversationViewState {
    pub fn item_by_id(&self, item_id: &str) -> Option<&ItemView> {
        self.items.iter().find(|item| item.id == item_id)
    }

    pub fn item_for_timeline_entry(&self, entry: &TimelineEntry) -> Option<&ItemView> {
        self.items
            .get(entry.item_index)
            .filter(|item| item.id == entry.item_id)
            .or_else(|| self.item_by_id(entry.item_id.as_str()))
    }
}

#[derive(Debug, Clone, Default)]
pub struct ConversationProjector {
    view_state: ConversationViewState,
    item_index: HashMap<String, usize>,
    item_timeline_index: HashMap<String, usize>,
    turn_index: HashMap<String, usize>,
    timeline_counter: u64,
    synthetic_item_counter: u64,
}

impl ConversationProjector {
    pub fn reset(&mut self) {
        self.view_state = ConversationViewState::default();
        self.item_index.clear();
        self.item_timeline_index.clear();
        self.turn_index.clear();
        self.timeline_counter = 0;
        self.synthetic_item_counter = 0;
    }

    pub fn view_state(&self) -> &ConversationViewState {
        &self.view_state
    }

    pub fn bump_revision(&mut self) {
        self.view_state.revision = self.view_state.revision.saturating_add(1);
    }

    pub fn set_item_timeline_origin(&mut self, item_id: &str, timeline_origin: TimelineOrigin) {
        if let Some(index) = self.item_index.get(item_id).copied()
            && let Some(item) = self.view_state.items.get_mut(index)
        {
            item.timeline_origin = Some(timeline_origin);
        }
    }

    pub fn item_count(&self) -> usize {
        self.view_state.items.len()
    }

    pub fn set_item_timeline_origin_from(
        &mut self,
        start_index: usize,
        timeline_origin: TimelineOrigin,
    ) {
        for item in self.view_state.items.iter_mut().skip(start_index) {
            item.timeline_origin = Some(timeline_origin.clone());
        }
    }

    pub fn sync_flow_state(&mut self, state_machine: &TurnStateMachine) {
        self.view_state.composer_locked = state_machine.is_locked();
        self.view_state.in_flight_turn_id = state_machine.in_flight_turn_id().map(str::to_owned);
        self.view_state.pending_request_id = state_machine.pending_request_id().map(str::to_owned);
        self.view_state.phase_label = state_machine.status_label().to_owned();
    }

    pub fn apply_local_turn_start_requested(
        &mut self,
        turn_id: &str,
        user_text: &str,
        attachments: &[UserMessageAttachment],
        ts_unix_ms: i64,
    ) {
        self.upsert_turn(turn_id, TurnPhase::Starting, Some(ts_unix_ms), None, None);
        self.view_state.last_error = None;

        if user_text.trim().is_empty() && attachments.is_empty() {
            return;
        }

        let item_id = format!("user_{turn_id}");
        let item = TurnItem::UserMessage {
            id: item_id.clone(),
            text: user_text.to_owned(),
            attachments: attachments.to_vec(),
        };
        self.start_item_view(
            item_id.as_str(),
            turn_id,
            "user_message",
            TimelineEntryStatus::Completed,
            user_text.to_owned(),
            None,
            item.clone(),
            None,
            ts_unix_ms,
        );
        self.complete_item_view(
            item_id.as_str(),
            TimelineEntryStatus::Completed,
            Some(user_text),
            None,
            item,
            None,
            ts_unix_ms,
        );
    }

    pub fn apply_local_turn_start_accepted(&mut self, turn_id: &str, ts_unix_ms: i64) {
        self.upsert_turn(turn_id, TurnPhase::Running, Some(ts_unix_ms), None, None);
    }

    pub fn apply_local_turn_start_rejected(&mut self, turn_id: &str, error: &str, ts_unix_ms: i64) {
        self.upsert_turn(
            turn_id,
            TurnPhase::Failed,
            None,
            Some(ts_unix_ms),
            Some(error.to_owned()),
        );
        self.view_state.last_error = Some(error.to_owned());
        self.mark_turn_items_terminal(turn_id, TimelineEntryStatus::Failed, ts_unix_ms);
        self.push_system_event_item(
            turn_id,
            SystemEventLevel::Error,
            error.to_owned(),
            Some("turn_start_rejected".to_owned()),
            None,
            ts_unix_ms,
        );
    }

    pub fn apply_local_turn_cancel_rejected(&mut self, error: &str) {
        self.view_state.last_error = Some(error.to_owned());
    }

    pub fn apply_turn_started(&mut self, turn: &Turn, ts_unix_ms: i64) {
        self.upsert_turn(
            turn.id.as_str(),
            TurnPhase::Running,
            Some(ts_unix_ms),
            None,
            turn.error.clone(),
        );
    }

    pub fn apply_turn_completed(&mut self, turn: &Turn, ts_unix_ms: i64) {
        self.upsert_turn(
            turn.id.as_str(),
            TurnPhase::Completing,
            None,
            None,
            turn.error.clone(),
        );
        self.mark_turn_items_terminal(turn.id.as_str(), TimelineEntryStatus::Completed, ts_unix_ms);
    }

    pub fn finalize_turn_completed(&mut self, turn_id: &str, ts_unix_ms: i64) {
        self.upsert_turn(turn_id, TurnPhase::Completed, None, Some(ts_unix_ms), None);
    }

    pub fn apply_turn_failed(&mut self, turn: &Turn, ts_unix_ms: i64) {
        let (phase, item_status, level, code) = if turn.status == TurnStatus::Interrupted {
            (
                TurnPhase::Cancelled,
                TimelineEntryStatus::Cancelled,
                SystemEventLevel::Warning,
                "turn_cancelled",
            )
        } else {
            (
                TurnPhase::Failed,
                TimelineEntryStatus::Failed,
                SystemEventLevel::Error,
                "turn_failed",
            )
        };

        self.upsert_turn(
            turn.id.as_str(),
            phase,
            None,
            Some(ts_unix_ms),
            turn.error.clone(),
        );
        self.mark_turn_items_terminal(turn.id.as_str(), item_status, ts_unix_ms);
        self.view_state.last_error = turn.error.clone();

        if let Some(error) = turn.error.as_deref()
            && !self.should_suppress_turn_failure_system_event(turn.id.as_str(), error)
        {
            self.push_system_event_item(
                turn.id.as_str(),
                level,
                turn_failure_system_message(error),
                Some(code.to_owned()),
                Some(serde_json::json!({
                    "error_message": error,
                })),
                ts_unix_ms,
            );
        }
    }

    pub fn apply_turn_blocked(
        &mut self,
        turn: &Turn,
        resume: Option<&TurnBlockedResumeMetadata>,
        ts_unix_ms: i64,
    ) {
        self.upsert_turn(
            turn.id.as_str(),
            TurnPhase::Blocked,
            None,
            Some(ts_unix_ms),
            turn.error.clone(),
        );
        if let Some(index) = self.turn_index.get(turn.id.as_str()).copied()
            && let Some(view) = self.view_state.turns.get_mut(index)
        {
            view.resume = resume.cloned();
        }
        self.mark_turn_items_terminal(turn.id.as_str(), TimelineEntryStatus::Blocked, ts_unix_ms);
        if let Some(resume) = resume {
            self.push_system_event_item(
                turn.id.as_str(),
                SystemEventLevel::Warning,
                format!("Turn blocked: {}", resume.human_message),
                Some("turn_blocked_resumable".to_owned()),
                Some(turn_blocked_resume_details(resume)),
                ts_unix_ms,
            );
        }
    }

    pub fn apply_item_timeout_detected(
        &mut self,
        turn_id: &str,
        item_id: &str,
        item_type: TurnItemType,
        attempt_no: u32,
        reason: TurnItemTimeoutReason,
        recovery_job_id: Option<&str>,
        ts_unix_ms: i64,
    ) {
        let reason_label = timeout_reason_label(reason);
        let recovery_message = if let Some(job_id) = recovery_job_id {
            client_system_label!(
                "timeline.system.timeout_delta_recovery_scheduled",
                attempt = attempt_no,
                reason = reason_label,
                job_id = job_id
            )
            .to_string()
        } else {
            client_system_label!(
                "timeline.system.timeout_delta_no_recovery",
                attempt = attempt_no,
                reason = reason_label
            )
            .to_string()
        };

        let timeout_delta = self
            .item_index
            .get(item_id)
            .and_then(|index| self.view_state.items.get(*index))
            .map(|item| !item.partial_text.is_empty())
            .unwrap_or(false)
            .then(|| format!("\n{recovery_message}"))
            .unwrap_or_else(|| recovery_message.clone());

        self.append_item_delta(item_id, timeout_delta.as_str(), None, ts_unix_ms);
        self.mark_item_terminal(item_id, TimelineEntryStatus::Failed, ts_unix_ms);

        let system_message = if recovery_job_id.is_some() {
            client_system_label!("timeline.system.timeout_with_recovery").to_string()
        } else {
            client_system_label!("timeline.system.timeout_without_recovery").to_string()
        };

        self.push_system_event_item(
            turn_id,
            SystemEventLevel::Warning,
            system_message,
            Some("item_timeout_detected".to_owned()),
            Some(serde_json::json!({
                "item_id": item_id,
                "item_type": turn_item_type_label(item_type),
                "attempt_no": attempt_no,
                "reason": reason_label,
                "recovery_job_id": recovery_job_id,
            })),
            ts_unix_ms,
        );
    }

    pub fn apply_item_recovery_opened(
        &mut self,
        turn_id: &str,
        item_id: &str,
        item_type: TurnItemType,
        recovery_job_id: &str,
        attempt_no: u32,
        ts_unix_ms: i64,
    ) {
        let message = client_system_label!("timeline.system.recovery_opened").to_string();
        self.push_system_event_item(
            turn_id,
            SystemEventLevel::Info,
            message,
            Some("item_recovery_opened".to_owned()),
            Some(serde_json::json!({
                "item_id": item_id,
                "item_type": turn_item_type_label(item_type),
                "recovery_job_id": recovery_job_id,
                "attempt_no": attempt_no,
            })),
            ts_unix_ms,
        );
    }

    pub fn apply_item_recovery_attached(
        &mut self,
        turn_id: &str,
        item_id: &str,
        item_type: TurnItemType,
        recovery_job_id: &str,
        recovery_item_id: &str,
        recovery_item_type: TurnItemType,
        existing_status: RecoveryJobStatus,
        next_attempt_no: u32,
        ts_unix_ms: i64,
    ) {
        let status_label = recovery_job_status_label(existing_status);
        let message = client_system_label!("timeline.system.recovery_attached").to_string();
        self.push_system_event_item(
            turn_id,
            SystemEventLevel::Info,
            message,
            Some("item_recovery_attached".to_owned()),
            Some(serde_json::json!({
                "item_id": item_id,
                "item_type": turn_item_type_label(item_type),
                "recovery_job_id": recovery_job_id,
                "recovery_item_id": recovery_item_id,
                "recovery_item_type": turn_item_type_label(recovery_item_type),
                "existing_status": status_label,
                "next_attempt_no": next_attempt_no,
            })),
            ts_unix_ms,
        );
    }

    pub fn apply_item_retry_scheduled(
        &mut self,
        turn_id: &str,
        item_id: &str,
        item_type: TurnItemType,
        recovery_job_id: &str,
        attempt_no: u32,
        next_run_at_unix: i64,
        reason: Option<&str>,
        ts_unix_ms: i64,
    ) {
        let message = client_system_label!("timeline.system.retry_scheduled").to_string();
        self.push_system_event_item(
            turn_id,
            SystemEventLevel::Warning,
            message,
            Some("item_retry_scheduled".to_owned()),
            Some(serde_json::json!({
                "item_id": item_id,
                "item_type": turn_item_type_label(item_type),
                "recovery_job_id": recovery_job_id,
                "attempt_no": attempt_no,
                "next_run_at_unix": next_run_at_unix,
                "reason": reason,
            })),
            ts_unix_ms,
        );
    }

    pub fn apply_item_retry_attempt_started(
        &mut self,
        turn_id: &str,
        item_id: &str,
        item_type: TurnItemType,
        recovery_job_id: &str,
        attempt_no: u32,
        ts_unix_ms: i64,
    ) {
        self.push_system_event_item(
            turn_id,
            SystemEventLevel::Info,
            client_system_label!("timeline.system.retry_started").to_string(),
            Some("item_retry_attempt_started".to_owned()),
            Some(serde_json::json!({
                "item_id": item_id,
                "item_type": turn_item_type_label(item_type),
                "recovery_job_id": recovery_job_id,
                "attempt_no": attempt_no,
            })),
            ts_unix_ms,
        );
    }

    pub fn apply_item_recovery_succeeded(
        &mut self,
        turn_id: &str,
        item_id: &str,
        item_type: TurnItemType,
        recovery_job_id: &str,
        attempt_no: u32,
        ts_unix_ms: i64,
    ) {
        self.push_system_event_item(
            turn_id,
            SystemEventLevel::Info,
            client_system_label!("timeline.system.recovery_succeeded").to_string(),
            Some("item_recovery_succeeded".to_owned()),
            Some(serde_json::json!({
                "item_id": item_id,
                "item_type": turn_item_type_label(item_type),
                "recovery_job_id": recovery_job_id,
                "attempt_no": attempt_no,
            })),
            ts_unix_ms,
        );
    }

    pub fn apply_item_recovery_exhausted(
        &mut self,
        turn_id: &str,
        item_id: &str,
        item_type: TurnItemType,
        recovery_job_id: &str,
        attempt_no: u32,
        status: RecoveryJobStatus,
        error_message: &str,
        ts_unix_ms: i64,
    ) {
        self.push_system_event_item(
            turn_id,
            SystemEventLevel::Error,
            client_system_label!("timeline.system.recovery_failed").to_string(),
            Some("item_recovery_exhausted".to_owned()),
            Some(serde_json::json!({
                "item_id": item_id,
                "item_type": turn_item_type_label(item_type),
                "recovery_job_id": recovery_job_id,
                "attempt_no": attempt_no,
                "status": format!("{status:?}"),
                "error_message": error_message,
            })),
            ts_unix_ms,
        );
    }

    pub fn apply_item_tool_retry_scheduled(
        &mut self,
        turn_id: &str,
        item_id: &str,
        item_type: TurnItemType,
        tool_retry_episode_id: &str,
        tool_name: &str,
        attempt_no: u32,
        error_class: ToolRetryErrorClass,
        retry_hint: &str,
        budgets: &[ToolRetryBudgetUsage],
        failure_signature_fingerprint: &str,
        reason: &str,
        ts_unix_ms: i64,
    ) {
        let error_class_label = tool_retry_error_class_label(error_class);
        let message = client_system_label!(
            "timeline.system.tool_retry_scheduled_with_attempt",
            tool_name = tool_name,
            attempt = attempt_no
        )
        .to_string();
        self.push_system_event_item(
            turn_id,
            SystemEventLevel::Warning,
            message,
            Some("item_tool_retry_scheduled".to_owned()),
            Some(serde_json::json!({
                "item_id": item_id,
                "item_type": turn_item_type_label(item_type),
                "tool_retry_episode_id": tool_retry_episode_id,
                "tool_name": tool_name,
                "attempt_no": attempt_no,
                "error_class": error_class_label,
                "retry_hint": retry_hint,
                "budgets": budgets,
                "failure_signature_fingerprint": failure_signature_fingerprint,
                "reason": reason,
            })),
            ts_unix_ms,
        );
    }

    pub fn apply_item_tool_retry_resolved(
        &mut self,
        turn_id: &str,
        item_id: &str,
        item_type: TurnItemType,
        tool_retry_episode_id: &str,
        tool_name: &str,
        attempt_no: u32,
        resolution: ToolRetryResolution,
        budgets: &[ToolRetryBudgetUsage],
        reason: &str,
        ts_unix_ms: i64,
    ) {
        let resolution_label = tool_retry_resolution_label(resolution);
        let message =
            client_system_label!("timeline.system.tool_retry_resolved", tool_name = tool_name)
                .to_string();
        self.push_system_event_item(
            turn_id,
            SystemEventLevel::Info,
            message,
            Some("item_tool_retry_resolved".to_owned()),
            Some(serde_json::json!({
                "item_id": item_id,
                "item_type": turn_item_type_label(item_type),
                "tool_retry_episode_id": tool_retry_episode_id,
                "tool_name": tool_name,
                "attempt_no": attempt_no,
                "resolution": resolution_label,
                "budgets": budgets,
                "reason": reason,
            })),
            ts_unix_ms,
        );
    }

    pub fn apply_item_tool_retry_exhausted(
        &mut self,
        turn_id: &str,
        item_id: &str,
        item_type: TurnItemType,
        tool_retry_episode_id: &str,
        tool_name: &str,
        attempt_no: u32,
        error_class: ToolRetryErrorClass,
        exhaustion_kind: ToolRetryExhaustionKind,
        budgets: &[ToolRetryBudgetUsage],
        failure_signature_fingerprint: &str,
        reason: &str,
        ts_unix_ms: i64,
    ) {
        let error_class_label = tool_retry_error_class_label(error_class);
        let exhaustion_kind_label = tool_retry_exhaustion_kind_label(exhaustion_kind);
        let message = client_system_label!(
            "timeline.system.tool_retry_exhausted",
            tool_name = tool_name
        )
        .to_string();
        self.push_system_event_item(
            turn_id,
            SystemEventLevel::Error,
            message,
            Some("item_tool_retry_exhausted".to_owned()),
            Some(serde_json::json!({
                "item_id": item_id,
                "item_type": turn_item_type_label(item_type),
                "tool_retry_episode_id": tool_retry_episode_id,
                "tool_name": tool_name,
                "attempt_no": attempt_no,
                "error_class": error_class_label,
                "exhaustion_kind": exhaustion_kind_label,
                "budgets": budgets,
                "failure_signature_fingerprint": failure_signature_fingerprint,
                "reason": reason,
            })),
            ts_unix_ms,
        );
    }

    pub fn apply_turn_tool_loop_budget_exceeded(
        &mut self,
        turn_id: &str,
        limit_kind: ToolLoopBudgetLimitKind,
        limit: u32,
        observed: u32,
        action: ToolLoopBudgetAction,
        reason: &str,
        ts_unix_ms: i64,
    ) {
        let limit_kind_label = tool_loop_budget_limit_kind_label(limit_kind);
        let action_label = tool_loop_budget_action_label(action);
        let level = match action {
            ToolLoopBudgetAction::ContinueInNextWindow => SystemEventLevel::Info,
        };
        let message = client_system_label!("timeline.system.tool_loop_budget_exceeded").to_string();
        self.push_system_event_item(
            turn_id,
            level,
            message,
            Some("turn_tool_loop_budget_exceeded".to_owned()),
            Some(serde_json::json!({
                "limit_kind": limit_kind_label,
                "limit": limit,
                "observed": observed,
                "action": action_label,
                "reason": reason,
            })),
            ts_unix_ms,
        );
    }

    pub fn apply_turn_execution_window_started(
        &mut self,
        _turn_id: &str,
        _notification: &TurnExecutionWindowStartedNotification,
        _ts_unix_ms: i64,
    ) {
        // Starting the first execution window is expected for every turn and is too noisy
        // for the user-facing timeline. The event remains in the event log and gateway DB.
    }

    pub fn apply_turn_execution_window_exhausted(
        &mut self,
        turn_id: &str,
        notification: &TurnExecutionWindowExhaustedNotification,
        ts_unix_ms: i64,
    ) {
        let exhaustion_reason =
            execution_window_exhaustion_reason_label(notification.exhaustion_reason);
        self.push_system_event_item_with_id(
            execution_window_transition_item_id(turn_id, notification.window_index),
            turn_id,
            SystemEventLevel::Warning,
            format!(
                "Execution window #{} exhausted: {}",
                notification.window_index, exhaustion_reason
            ),
            Some("turn_execution_window_exhausted".to_owned()),
            Some(serde_json::json!({
                "workspace_id": notification.workspace_id.as_str(),
                "thread_id": notification.thread_id.as_str(),
                "turn_id": notification.turn_id.as_str(),
                "window_id": notification.window_id.as_str(),
                "window_index": notification.window_index,
                "status": execution_window_status_label(notification.status),
                "exhaustion_reason": exhaustion_reason,
                "limit": notification.limit,
                "observed": notification.observed,
                "agent_round_count": notification.agent_round_count,
                "tool_call_count": notification.tool_call_count,
                "provider_token_count": notification.provider_token_count,
                "reason": notification.reason.as_str(),
            })),
            ts_unix_ms,
        );
    }

    pub fn apply_turn_execution_window_checkpointed(
        &mut self,
        _turn_id: &str,
        _notification: &TurnExecutionWindowCheckpointedNotification,
        _ts_unix_ms: i64,
    ) {
        // Checkpoint persistence is an internal continuation detail. Keep it in the
        // durable event stream, but do not create a standalone timeline item.
    }

    pub fn apply_turn_execution_window_continued(
        &mut self,
        turn_id: &str,
        notification: &TurnExecutionWindowContinuedNotification,
        ts_unix_ms: i64,
    ) {
        let item_id =
            execution_window_transition_item_id(turn_id, notification.previous_window_index);
        let mut details = self
            .system_event_details_for_item(item_id.as_str())
            .unwrap_or_else(|| serde_json::json!({}));
        merge_json_object(
            &mut details,
            serde_json::json!({
                "workspace_id": notification.workspace_id.as_str(),
                "thread_id": notification.thread_id.as_str(),
                "turn_id": notification.turn_id.as_str(),
                "window_id": notification.window_id.as_str(),
                "window_index": notification.window_index,
                "status": execution_window_status_label(notification.status),
                "previous_window_id": notification.previous_window_id.as_str(),
                "previous_window_index": notification.previous_window_index,
                "checkpoint_id": notification.checkpoint_id.as_str(),
            }),
        );
        self.push_system_event_item_with_id(
            item_id,
            turn_id,
            SystemEventLevel::Info,
            format!(
                "Continued in execution window #{} after window #{} limit",
                notification.window_index, notification.previous_window_index
            ),
            Some("turn_execution_window_continued".to_owned()),
            Some(details),
            ts_unix_ms,
        );
    }

    pub fn apply_turn_execution_window_blocked(
        &mut self,
        turn_id: &str,
        notification: &TurnExecutionWindowBlockedNotification,
        ts_unix_ms: i64,
    ) {
        self.upsert_turn(
            turn_id,
            TurnPhase::Blocked,
            None,
            Some(ts_unix_ms),
            Some(notification.reason.clone()),
        );
        self.mark_turn_items_terminal(turn_id, TimelineEntryStatus::Blocked, ts_unix_ms);
        self.push_system_event_item(
            turn_id,
            SystemEventLevel::Warning,
            format!("Execution paused: {}", notification.reason),
            Some("turn_execution_window_blocked".to_owned()),
            Some(serde_json::json!({
                "workspace_id": notification.workspace_id.as_str(),
                "thread_id": notification.thread_id.as_str(),
                "turn_id": notification.turn_id.as_str(),
                "window_id": notification.window_id.as_str(),
                "window_index": notification.window_index,
                "status": execution_window_status_label(notification.status),
                "exhaustion_reason": notification
                    .exhaustion_reason
                    .map(execution_window_exhaustion_reason_label),
                "checkpoint_id": notification.checkpoint_id.as_deref(),
                "total_windows": notification.total_windows,
                "total_tool_calls": notification.total_tool_calls,
                "reason": notification.reason.as_str(),
            })),
            ts_unix_ms,
        );
    }

    pub fn item_type_by_id(&self, item_id: &str) -> Option<&str> {
        let index = self.item_index.get(item_id).copied()?;
        self.view_state
            .items
            .get(index)
            .map(|item| item.item_type.as_str())
    }

    pub fn start_item_view(
        &mut self,
        item_id: &str,
        turn_id: &str,
        item_type: &str,
        status: TimelineEntryStatus,
        text: String,
        markdown: Option<MarkdownDocument>,
        item_payload: TurnItem,
        opaque_meta: Option<JsonValue>,
        ts_unix_ms: i64,
    ) {
        let can_mark_running = self
            .turn_index
            .get(turn_id)
            .and_then(|index| self.view_state.turns.get(*index))
            .is_none_or(|turn| matches!(turn.phase, TurnPhase::Starting | TurnPhase::Running));
        if can_mark_running {
            self.upsert_turn(turn_id, TurnPhase::Running, Some(ts_unix_ms), None, None);
        }

        let item_index = if let Some(index) = self.item_index.get(item_id).copied()
            && let Some(item) = self.view_state.items.get_mut(index)
        {
            let preserve_terminal =
                is_terminal_timeline_status(item.status) && status == TimelineEntryStatus::Running;
            if !preserve_terminal {
                item.status = status;
            }
            item.started_at_unix_ms = item.started_at_unix_ms.or(Some(ts_unix_ms));
            if !preserve_terminal {
                item.updated_at_unix_ms = Some(ts_unix_ms);
            }
            if !text.is_empty() && (!preserve_terminal || item.partial_text.is_empty()) {
                item.partial_text = text.clone();
            }
            if let Some(markdown) = markdown.clone()
                && (!preserve_terminal || item.partial_markdown.is_none())
            {
                item.partial_markdown = Some(markdown);
            }
            if !preserve_terminal {
                item.item_type = item_type.to_owned();
                item.item = item_payload.clone();
            }
            merge_item_opaque_meta(&mut item.opaque_meta, opaque_meta.clone());
            index
        } else {
            self.view_state.items.push(ItemView {
                id: item_id.to_owned(),
                turn_id: turn_id.to_owned(),
                item_type: item_type.to_owned(),
                status,
                started_at_unix_ms: Some(ts_unix_ms),
                updated_at_unix_ms: Some(ts_unix_ms),
                completed_at_unix_ms: None,
                partial_text: text.clone(),
                final_text: None,
                partial_markdown: markdown.clone(),
                final_markdown: None,
                item: item_payload.clone(),
                timeline_origin: None,
                opaque_meta: opaque_meta.clone(),
            });
            let index = self.view_state.items.len().saturating_sub(1);
            self.item_index.insert(item_id.to_owned(), index);
            index
        };

        if let Some(index) = self.item_timeline_index.get(item_id).copied()
            && let Some(entry) = self.view_state.timeline.get_mut(index)
        {
            entry.item_index = item_index;
            return;
        }

        let timeline_entry = TimelineEntry {
            id: self.next_timeline_entry_id(),
            turn_id: turn_id.to_owned(),
            item_id: item_id.to_owned(),
            item_index,
        };
        self.insert_timeline_entry(timeline_entry);
    }

    pub fn append_item_delta(
        &mut self,
        item_id: &str,
        delta: &str,
        markdown: Option<MarkdownDocument>,
        ts_unix_ms: i64,
    ) {
        if delta.is_empty() {
            return;
        }

        if let Some(index) = self.item_index.get(item_id).copied()
            && let Some(item) = self.view_state.items.get_mut(index)
        {
            let preserve_terminal = is_terminal_timeline_status(item.status);
            item.partial_text.push_str(delta);
            if !preserve_terminal {
                item.status = TimelineEntryStatus::Running;
                item.updated_at_unix_ms = Some(ts_unix_ms);
            }
            if let Some(markdown) = markdown.clone() {
                item.partial_markdown = Some(markdown);
            }
        }
    }

    pub fn complete_item_view(
        &mut self,
        item_id: &str,
        status: TimelineEntryStatus,
        final_text: Option<&str>,
        final_markdown: Option<MarkdownDocument>,
        item_payload: TurnItem,
        opaque_meta: Option<JsonValue>,
        completed_at_unix_ms: i64,
    ) {
        if let Some(index) = self.item_index.get(item_id).copied()
            && let Some(item) = self.view_state.items.get_mut(index)
        {
            item.status = status;
            item.updated_at_unix_ms = Some(completed_at_unix_ms);
            item.completed_at_unix_ms = Some(completed_at_unix_ms);
            if let Some(text) = final_text
                && !text.is_empty()
            {
                item.final_text = Some(text.to_owned());
                item.partial_text = text.to_owned();
            }
            if let Some(markdown) = final_markdown.clone() {
                item.final_markdown = Some(markdown.clone());
                item.partial_markdown = Some(markdown);
            }
            item.item = item_payload.clone();
            merge_item_opaque_meta(&mut item.opaque_meta, opaque_meta.clone());
        }
    }

    fn upsert_turn(
        &mut self,
        turn_id: &str,
        phase: TurnPhase,
        started_at_unix_ms: Option<i64>,
        completed_at_unix_ms: Option<i64>,
        error: Option<String>,
    ) {
        if let Some(index) = self.turn_index.get(turn_id).copied()
            && let Some(turn) = self.view_state.turns.get_mut(index)
        {
            turn.phase = phase;
            if started_at_unix_ms.is_some() {
                turn.started_at_unix_ms = started_at_unix_ms;
            }
            if completed_at_unix_ms.is_some() {
                turn.completed_at_unix_ms = completed_at_unix_ms;
            }
            if error.is_some() {
                turn.error = error;
            }
            return;
        }

        self.view_state.turns.push(TurnView {
            id: turn_id.to_owned(),
            phase,
            started_at_unix_ms,
            completed_at_unix_ms,
            error,
            resume: None,
        });
        let index = self.view_state.turns.len().saturating_sub(1);
        self.turn_index.insert(turn_id.to_owned(), index);
    }

    fn mark_turn_items_terminal(
        &mut self,
        turn_id: &str,
        terminal_status: TimelineEntryStatus,
        ts_unix_ms: i64,
    ) {
        for item in &mut self.view_state.items {
            if item.turn_id != turn_id {
                continue;
            }
            if matches!(item.status, TimelineEntryStatus::Running) {
                item.status = terminal_status;
                item.updated_at_unix_ms = Some(ts_unix_ms);
                item.completed_at_unix_ms = Some(ts_unix_ms);
            }
        }
    }

    fn mark_item_terminal(
        &mut self,
        item_id: &str,
        terminal_status: TimelineEntryStatus,
        ts_unix_ms: i64,
    ) {
        if let Some(index) = self.item_index.get(item_id).copied()
            && let Some(item) = self.view_state.items.get_mut(index)
            && item.status == TimelineEntryStatus::Running
        {
            item.status = terminal_status;
            item.updated_at_unix_ms = Some(ts_unix_ms);
            item.completed_at_unix_ms = Some(ts_unix_ms);
        }
    }

    fn should_suppress_turn_failure_system_event(&self, turn_id: &str, error: &str) -> bool {
        is_recovery_failure_error(error)
            && self.has_system_event_code(turn_id, "item_recovery_exhausted")
    }

    fn has_system_event_code(&self, turn_id: &str, code: &str) -> bool {
        self.view_state.items.iter().any(|item| {
            item.turn_id == turn_id
                && matches!(
                    &item.item,
                    TurnItem::SystemEvent {
                        code: Some(existing),
                        ..
                    } if existing == code
                )
        })
    }

    fn push_system_event_item(
        &mut self,
        turn_id: &str,
        level: SystemEventLevel,
        message: String,
        code: Option<String>,
        details: Option<JsonValue>,
        ts_unix_ms: i64,
    ) {
        let item_id = stable_system_event_item_id(turn_id, code.as_deref(), details.as_ref())
            .unwrap_or_else(|| self.next_synthetic_item_id("system_event"));
        self.push_system_event_item_with_id(
            item_id, turn_id, level, message, code, details, ts_unix_ms,
        );
    }

    fn push_system_event_item_with_id(
        &mut self,
        item_id: String,
        turn_id: &str,
        level: SystemEventLevel,
        message: String,
        code: Option<String>,
        details: Option<JsonValue>,
        ts_unix_ms: i64,
    ) {
        let item = TurnItem::SystemEvent {
            id: item_id.clone(),
            level,
            message: message.clone(),
            code,
            details: details.clone(),
        };

        self.start_item_view(
            item_id.as_str(),
            turn_id,
            "system_event",
            TimelineEntryStatus::Completed,
            message.clone(),
            None,
            item.clone(),
            details.clone(),
            ts_unix_ms,
        );
        self.complete_item_view(
            item_id.as_str(),
            TimelineEntryStatus::Completed,
            Some(message.as_str()),
            None,
            item,
            details,
            ts_unix_ms,
        );
    }

    fn system_event_details_for_item(&self, item_id: &str) -> Option<JsonValue> {
        let index = self.item_index.get(item_id).copied()?;
        let item = self.view_state.items.get(index)?;
        match &item.item {
            TurnItem::SystemEvent {
                details: Some(details),
                ..
            } => Some(details.clone()),
            _ => item.opaque_meta.clone(),
        }
    }

    fn next_timeline_entry_id(&mut self) -> String {
        self.timeline_counter = self.timeline_counter.saturating_add(1);
        format!("timeline_{:016x}", self.timeline_counter)
    }

    fn insert_timeline_entry(&mut self, timeline_entry: TimelineEntry) {
        let insert_index = self
            .view_state
            .timeline
            .iter()
            .position(|entry| self.timeline_entry_cmp(&timeline_entry, entry).is_lt())
            .unwrap_or(self.view_state.timeline.len());

        self.view_state
            .timeline
            .insert(insert_index, timeline_entry);
        self.rebuild_item_timeline_index();
    }

    fn rebuild_item_timeline_index(&mut self) {
        self.item_timeline_index.clear();
        for (index, entry) in self.view_state.timeline.iter().enumerate() {
            self.item_timeline_index
                .insert(entry.item_id.clone(), index);
        }
    }

    fn timeline_entry_sort_at(&self, entry: &TimelineEntry) -> i64 {
        self.view_state
            .items
            .get(entry.item_index)
            .filter(|item| item.id == entry.item_id)
            .or_else(|| self.view_state.item_by_id(entry.item_id.as_str()))
            .map(item_sort_at)
            .unwrap_or(i64::MAX)
    }

    fn timeline_entry_cmp(&self, left: &TimelineEntry, right: &TimelineEntry) -> Ordering {
        if left.turn_id == right.turn_id {
            let left_rank = self.timeline_entry_kind_rank(left);
            let right_rank = self.timeline_entry_kind_rank(right);
            if left_rank != right_rank {
                return left_rank.cmp(&right_rank);
            }
        }

        self.timeline_entry_sort_at(left)
            .cmp(&self.timeline_entry_sort_at(right))
    }

    fn timeline_entry_kind_rank(&self, entry: &TimelineEntry) -> u8 {
        self.view_state
            .items
            .get(entry.item_index)
            .filter(|item| item.id == entry.item_id)
            .or_else(|| self.view_state.item_by_id(entry.item_id.as_str()))
            .map(|item| {
                if matches!(&item.item, TurnItem::UserMessage { .. }) {
                    0
                } else {
                    1
                }
            })
            .unwrap_or(1)
    }

    fn next_synthetic_item_id(&mut self, prefix: &str) -> String {
        self.synthetic_item_counter = self.synthetic_item_counter.saturating_add(1);
        format!("{prefix}_{:016x}", self.synthetic_item_counter)
    }
}

fn item_sort_at(item: &ItemView) -> i64 {
    item.started_at_unix_ms
        .or(item.updated_at_unix_ms)
        .or(item.completed_at_unix_ms)
        .unwrap_or(i64::MAX)
}

fn is_terminal_timeline_status(status: TimelineEntryStatus) -> bool {
    !matches!(status, TimelineEntryStatus::Running)
}

fn timeout_reason_label(reason: TurnItemTimeoutReason) -> &'static str {
    match reason {
        TurnItemTimeoutReason::StartDeadlineExceeded => "start deadline exceeded",
        TurnItemTimeoutReason::IdleDeadlineExceeded => "idle deadline exceeded",
        TurnItemTimeoutReason::HardDeadlineExceeded => "hard deadline exceeded",
        TurnItemTimeoutReason::LeaseExpired => "lease expired",
    }
}

fn turn_blocked_resume_details(resume: &TurnBlockedResumeMetadata) -> JsonValue {
    serde_json::json!({
        "reason_class": &resume.reason_class,
        "human_message": &resume.human_message,
        "resume_requirements": &resume.resume_requirements,
        "resume_command": &resume.resume_command,
        "blocked_recovery_job_id": &resume.blocked_recovery_job_id,
        "latest_checkpoint_id": &resume.latest_checkpoint_id,
        "can_resume_same_turn": resume.can_resume_same_turn,
    })
}

fn turn_item_type_label(item_type: TurnItemType) -> &'static str {
    match item_type {
        TurnItemType::UserMessage => "user_message",
        TurnItemType::AgentMessage => "agent_message",
        TurnItemType::Reasoning => "reasoning",
        TurnItemType::SystemEvent => "system_event",
        TurnItemType::Task => "task",
        TurnItemType::CommandExecution => "command_execution",
        TurnItemType::FileChange => "file_change",
        TurnItemType::WebSearch => "web_search",
        TurnItemType::WebFetch => "web_fetch",
        TurnItemType::Download => "download",
        TurnItemType::DynamicToolCall => "dynamic_tool_call",
    }
}

fn recovery_job_status_label(status: RecoveryJobStatus) -> &'static str {
    match status {
        RecoveryJobStatus::Pending => "pending",
        RecoveryJobStatus::Active => "active",
        RecoveryJobStatus::Succeeded => "succeeded",
        RecoveryJobStatus::Failed => "failed",
        RecoveryJobStatus::Exhausted => "exhausted",
        RecoveryJobStatus::Blocked => "blocked",
        RecoveryJobStatus::Cancelled => "cancelled",
    }
}

fn stable_system_event_item_id(
    turn_id: &str,
    code: Option<&str>,
    details: Option<&JsonValue>,
) -> Option<String> {
    let code = code?;
    let details = details?;
    let mut hasher = DefaultHasher::new();
    turn_id.hash(&mut hasher);
    code.hash(&mut hasher);
    details.to_string().hash(&mut hasher);
    Some(format!("system_event_{:016x}", hasher.finish()))
}

fn execution_window_transition_item_id(turn_id: &str, window_index: u32) -> String {
    let mut hasher = DefaultHasher::new();
    turn_id.hash(&mut hasher);
    "execution_window_transition".hash(&mut hasher);
    window_index.hash(&mut hasher);
    format!("system_event_{:016x}", hasher.finish())
}

fn merge_json_object(target: &mut JsonValue, incoming: JsonValue) {
    match (target.as_object_mut(), incoming) {
        (Some(target), JsonValue::Object(incoming)) => {
            for (key, value) in incoming {
                target.insert(key, value);
            }
        }
        (_, incoming) => {
            *target = incoming;
        }
    }
}

fn merge_item_opaque_meta(existing: &mut Option<JsonValue>, incoming: Option<JsonValue>) {
    let Some(incoming) = incoming else {
        return;
    };
    *existing = Some(incoming);
}

fn is_recovery_failure_error(error: &str) -> bool {
    error.starts_with("recovery failed for item `")
}

fn turn_failure_system_message(error: &str) -> String {
    if is_recovery_failure_error(error) {
        client_system_label!("timeline.system.recovery_failed").to_string()
    } else {
        error.to_owned()
    }
}

fn tool_retry_error_class_label(error_class: ToolRetryErrorClass) -> &'static str {
    match error_class {
        ToolRetryErrorClass::InvalidArguments => "invalid_arguments",
        ToolRetryErrorClass::NotFound => "not_found",
        ToolRetryErrorClass::ToolNotVisible => "tool_not_visible",
        ToolRetryErrorClass::PermissionDenied => "permission_denied",
        ToolRetryErrorClass::CommandNotFound => "command_not_found",
        ToolRetryErrorClass::Timeout => "timeout",
        ToolRetryErrorClass::Cancelled => "cancelled",
        ToolRetryErrorClass::ExecutionFailed => "execution_failed",
        ToolRetryErrorClass::NeedsNarrowing => "needs_narrowing",
        ToolRetryErrorClass::Internal => "internal",
        ToolRetryErrorClass::OutputTruncated => "output_truncated",
        ToolRetryErrorClass::Unknown => "unknown",
    }
}

fn tool_retry_resolution_label(resolution: ToolRetryResolution) -> &'static str {
    match resolution {
        ToolRetryResolution::Succeeded => "succeeded",
        ToolRetryResolution::NonRetryable => "non_retryable",
    }
}

fn tool_retry_exhaustion_kind_label(exhaustion_kind: ToolRetryExhaustionKind) -> &'static str {
    match exhaustion_kind {
        ToolRetryExhaustionKind::TotalRetryRounds => "total_retry_rounds",
        ToolRetryExhaustionKind::ErrorClass => "error_class",
        ToolRetryExhaustionKind::ToolName => "tool_name",
        ToolRetryExhaustionKind::FailureSignature => "failure_signature",
    }
}

fn tool_loop_budget_limit_kind_label(limit_kind: ToolLoopBudgetLimitKind) -> &'static str {
    match limit_kind {
        ToolLoopBudgetLimitKind::AgentRounds => "agent_rounds",
        ToolLoopBudgetLimitKind::ToolCalls => "tool_calls",
        ToolLoopBudgetLimitKind::ProviderReturnedToolsAfterToolsDisabled => {
            "provider_returned_tools_after_tools_disabled"
        }
    }
}

fn tool_loop_budget_action_label(action: ToolLoopBudgetAction) -> &'static str {
    match action {
        ToolLoopBudgetAction::ContinueInNextWindow => "continue_in_next_window",
    }
}

fn execution_window_status_label(status: ExecutionWindowStatus) -> &'static str {
    match status {
        ExecutionWindowStatus::Running => "running",
        ExecutionWindowStatus::Exhausted => "exhausted",
        ExecutionWindowStatus::Checkpointed => "checkpointed",
        ExecutionWindowStatus::Continued => "continued",
        ExecutionWindowStatus::Completed => "completed",
        ExecutionWindowStatus::Interrupted => "interrupted",
        ExecutionWindowStatus::Blocked => "blocked",
        ExecutionWindowStatus::Failed => "failed",
    }
}

fn execution_window_exhaustion_reason_label(
    reason: ExecutionWindowExhaustionReason,
) -> &'static str {
    match reason {
        ExecutionWindowExhaustionReason::MaxAgentRoundsPerWindow => "max_agent_rounds_per_window",
        ExecutionWindowExhaustionReason::MaxToolCallsPerWindow => "max_tool_calls_per_window",
        ExecutionWindowExhaustionReason::MaxWallClockMsPerWindow => "max_wall_clock_ms_per_window",
        ExecutionWindowExhaustionReason::MaxProviderTokensPerWindow => {
            "max_provider_tokens_per_window"
        }
        ExecutionWindowExhaustionReason::ProviderFailureContinuation => {
            "provider_failure_continuation"
        }
        ExecutionWindowExhaustionReason::RuntimeShutdownContinuation => {
            "runtime_shutdown_continuation"
        }
    }
}
