use anyhow::Result;
use pioneer_protocol::{
    TaskConcurrencyConflictPolicy, TaskExecutorKind, TaskOwnerKind, TaskResultCandidateStatus,
    TaskResultReviewDecision, TaskResultReviewEventKind, TaskResultReviewerKind,
    TaskRunExecutionStatus, TaskRunStatus, TaskRunThreadBindingKind, TaskRunTurnKind,
    TaskRunTurnStatus, TaskStatus, TaskTriggerKind, TaskTriggerStatus, TaskWriteLockScopeKind,
    TaskWriteLockStatus,
};
use serde::{Serialize, de::DeserializeOwned};

pub fn task_status_to_db(status: TaskStatus) -> &'static str {
    match status {
        TaskStatus::Draft => "draft",
        TaskStatus::Scheduled => "scheduled",
        TaskStatus::Queued => "queued",
        TaskStatus::Running => "running",
        TaskStatus::Waiting => "waiting",
        TaskStatus::WaitingReview => "waiting_review",
        TaskStatus::Completed => "completed",
        TaskStatus::Failed => "failed",
        TaskStatus::Blocked => "blocked",
        TaskStatus::Cancelled => "cancelled",
    }
}

pub fn task_status_from_db(value: &str) -> Option<TaskStatus> {
    match value {
        "draft" => Some(TaskStatus::Draft),
        "scheduled" => Some(TaskStatus::Scheduled),
        "queued" => Some(TaskStatus::Queued),
        "running" => Some(TaskStatus::Running),
        "waiting" => Some(TaskStatus::Waiting),
        "waiting_review" => Some(TaskStatus::WaitingReview),
        "completed" => Some(TaskStatus::Completed),
        "failed" => Some(TaskStatus::Failed),
        "blocked" => Some(TaskStatus::Blocked),
        "cancelled" => Some(TaskStatus::Cancelled),
        _ => None,
    }
}

pub fn is_terminal_task_status(status: TaskStatus) -> bool {
    status.is_terminal()
}

pub fn is_terminal_task_status_db(status: &str) -> bool {
    task_status_from_db(status).is_some_and(TaskStatus::is_terminal)
}

pub fn task_owner_kind_to_db(kind: TaskOwnerKind) -> &'static str {
    match kind {
        TaskOwnerKind::User => "user",
        TaskOwnerKind::Thread => "thread",
        TaskOwnerKind::Workspace => "workspace",
        TaskOwnerKind::System => "system",
    }
}

pub fn task_owner_kind_from_db(value: &str) -> Option<TaskOwnerKind> {
    match value {
        "user" => Some(TaskOwnerKind::User),
        "thread" => Some(TaskOwnerKind::Thread),
        "workspace" => Some(TaskOwnerKind::Workspace),
        "system" => Some(TaskOwnerKind::System),
        _ => None,
    }
}

pub fn task_run_status_to_db(status: TaskRunStatus) -> &'static str {
    match status {
        TaskRunStatus::Queued => "queued",
        TaskRunStatus::Starting => "starting",
        TaskRunStatus::Running => "running",
        TaskRunStatus::Waiting => "waiting",
        TaskRunStatus::WaitingReview => "waiting_review",
        TaskRunStatus::Succeeded => "succeeded",
        TaskRunStatus::Failed => "failed",
        TaskRunStatus::Blocked => "blocked",
        TaskRunStatus::Cancelled => "cancelled",
        TaskRunStatus::TimedOut => "timed_out",
    }
}

pub fn task_run_status_from_db(value: &str) -> Option<TaskRunStatus> {
    match value {
        "queued" => Some(TaskRunStatus::Queued),
        "starting" => Some(TaskRunStatus::Starting),
        "running" => Some(TaskRunStatus::Running),
        "waiting" => Some(TaskRunStatus::Waiting),
        "waiting_review" => Some(TaskRunStatus::WaitingReview),
        "succeeded" => Some(TaskRunStatus::Succeeded),
        "failed" => Some(TaskRunStatus::Failed),
        "blocked" => Some(TaskRunStatus::Blocked),
        "cancelled" => Some(TaskRunStatus::Cancelled),
        "timed_out" => Some(TaskRunStatus::TimedOut),
        _ => None,
    }
}

pub fn task_trigger_kind_to_db(kind: TaskTriggerKind) -> &'static str {
    match kind {
        TaskTriggerKind::Immediate => "immediate",
        TaskTriggerKind::ScheduledAt => "scheduled_at",
        TaskTriggerKind::Interval => "interval",
        TaskTriggerKind::Cron => "cron",
        TaskTriggerKind::Manual => "manual",
        TaskTriggerKind::External => "external",
        TaskTriggerKind::Dependency => "dependency",
    }
}

pub fn task_trigger_kind_from_db(value: &str) -> Option<TaskTriggerKind> {
    match value {
        "immediate" => Some(TaskTriggerKind::Immediate),
        "scheduled_at" => Some(TaskTriggerKind::ScheduledAt),
        "interval" => Some(TaskTriggerKind::Interval),
        "cron" => Some(TaskTriggerKind::Cron),
        "manual" => Some(TaskTriggerKind::Manual),
        "external" => Some(TaskTriggerKind::External),
        "dependency" => Some(TaskTriggerKind::Dependency),
        _ => None,
    }
}

pub fn task_trigger_status_to_db(status: TaskTriggerStatus) -> &'static str {
    match status {
        TaskTriggerStatus::Active => "active",
        TaskTriggerStatus::Paused => "paused",
        TaskTriggerStatus::Exhausted => "exhausted",
        TaskTriggerStatus::Cancelled => "cancelled",
    }
}

pub fn task_trigger_status_from_db(value: &str) -> Option<TaskTriggerStatus> {
    match value {
        "active" => Some(TaskTriggerStatus::Active),
        "paused" => Some(TaskTriggerStatus::Paused),
        "exhausted" => Some(TaskTriggerStatus::Exhausted),
        "cancelled" => Some(TaskTriggerStatus::Cancelled),
        _ => None,
    }
}

pub fn task_executor_kind_to_db(kind: TaskExecutorKind) -> &'static str {
    match kind {
        TaskExecutorKind::Agent => "agent",
        TaskExecutorKind::Tool => "tool",
        TaskExecutorKind::Workflow => "workflow",
        TaskExecutorKind::Webhook => "webhook",
        TaskExecutorKind::System => "system",
    }
}

pub fn task_executor_kind_from_db(value: &str) -> Option<TaskExecutorKind> {
    match value {
        "agent" => Some(TaskExecutorKind::Agent),
        "tool" => Some(TaskExecutorKind::Tool),
        "workflow" => Some(TaskExecutorKind::Workflow),
        "webhook" => Some(TaskExecutorKind::Webhook),
        "system" => Some(TaskExecutorKind::System),
        _ => None,
    }
}

pub fn is_terminal_task_run_status(status: TaskRunStatus) -> bool {
    status.is_terminal()
}

pub fn task_run_execution_status_to_db(status: TaskRunExecutionStatus) -> &'static str {
    match status {
        TaskRunExecutionStatus::Reserved => "reserved",
        TaskRunExecutionStatus::Starting => "starting",
        TaskRunExecutionStatus::Running => "running",
        TaskRunExecutionStatus::WaitingReview => "waiting_review",
        TaskRunExecutionStatus::Succeeded => "succeeded",
        TaskRunExecutionStatus::Failed => "failed",
        TaskRunExecutionStatus::Blocked => "blocked",
        TaskRunExecutionStatus::Cancelled => "cancelled",
        TaskRunExecutionStatus::TimedOut => "timed_out",
    }
}

pub fn task_run_execution_status_from_db(value: &str) -> Option<TaskRunExecutionStatus> {
    match value {
        "reserved" => Some(TaskRunExecutionStatus::Reserved),
        "starting" => Some(TaskRunExecutionStatus::Starting),
        "running" => Some(TaskRunExecutionStatus::Running),
        "waiting_review" => Some(TaskRunExecutionStatus::WaitingReview),
        "succeeded" => Some(TaskRunExecutionStatus::Succeeded),
        "failed" => Some(TaskRunExecutionStatus::Failed),
        "blocked" => Some(TaskRunExecutionStatus::Blocked),
        "cancelled" => Some(TaskRunExecutionStatus::Cancelled),
        "timed_out" => Some(TaskRunExecutionStatus::TimedOut),
        _ => None,
    }
}

pub fn is_terminal_task_run_execution_status(status: TaskRunExecutionStatus) -> bool {
    status.is_terminal()
}

fn enum_to_snake_string<T: Serialize>(value: T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .expect("protocol enum should serialize as a string")
}

fn enum_from_snake_string<T: DeserializeOwned>(value: &str, type_name: &str) -> Result<T> {
    serde_json::from_value(serde_json::Value::String(value.to_owned()))
        .map_err(|err| anyhow::anyhow!("unknown {type_name} `{value}`: {err}"))
}

pub fn task_run_thread_binding_kind_to_db(kind: TaskRunThreadBindingKind) -> String {
    enum_to_snake_string(kind)
}

pub fn task_run_thread_binding_kind_from_db(value: &str) -> Result<TaskRunThreadBindingKind> {
    enum_from_snake_string(value, "task run thread binding kind")
}

pub fn task_run_turn_kind_to_db(kind: TaskRunTurnKind) -> String {
    enum_to_snake_string(kind)
}

pub fn task_run_turn_kind_from_db(value: &str) -> Result<TaskRunTurnKind> {
    enum_from_snake_string(value, "task run turn kind")
}

pub fn task_run_turn_status_to_db(status: TaskRunTurnStatus) -> String {
    enum_to_snake_string(status)
}

pub fn task_run_turn_status_from_db(value: &str) -> Result<TaskRunTurnStatus> {
    enum_from_snake_string(value, "task run turn status")
}

pub fn task_result_candidate_status_to_db(status: TaskResultCandidateStatus) -> String {
    enum_to_snake_string(status)
}

pub fn task_result_candidate_status_from_db(value: &str) -> Result<TaskResultCandidateStatus> {
    enum_from_snake_string(value, "task result candidate status")
}

pub fn task_result_reviewer_kind_to_db(kind: TaskResultReviewerKind) -> String {
    enum_to_snake_string(kind)
}

pub fn task_result_reviewer_kind_from_db(value: &str) -> Result<TaskResultReviewerKind> {
    enum_from_snake_string(value, "task result reviewer kind")
}

pub fn task_result_review_event_kind_to_db(kind: TaskResultReviewEventKind) -> String {
    enum_to_snake_string(kind)
}

pub fn task_result_review_event_kind_from_db(value: &str) -> Result<TaskResultReviewEventKind> {
    enum_from_snake_string(value, "task result review event kind")
}

pub fn task_result_review_decision_to_db(decision: TaskResultReviewDecision) -> String {
    enum_to_snake_string(decision)
}

pub fn task_result_review_decision_from_db(value: &str) -> Result<TaskResultReviewDecision> {
    enum_from_snake_string(value, "task result review decision")
}

pub fn task_concurrency_conflict_policy_to_db(
    policy: TaskConcurrencyConflictPolicy,
) -> &'static str {
    match policy {
        TaskConcurrencyConflictPolicy::Queue => "queue",
        TaskConcurrencyConflictPolicy::Reject => "reject",
        TaskConcurrencyConflictPolicy::CancelExisting => "cancel_existing",
        TaskConcurrencyConflictPolicy::Allow => "allow",
    }
}

pub fn task_concurrency_conflict_policy_from_db(
    value: &str,
) -> Option<TaskConcurrencyConflictPolicy> {
    match value {
        "queue" => Some(TaskConcurrencyConflictPolicy::Queue),
        "reject" => Some(TaskConcurrencyConflictPolicy::Reject),
        "cancel_existing" => Some(TaskConcurrencyConflictPolicy::CancelExisting),
        "allow" => Some(TaskConcurrencyConflictPolicy::Allow),
        _ => None,
    }
}

pub fn task_write_lock_scope_kind_to_db(kind: TaskWriteLockScopeKind) -> &'static str {
    match kind {
        TaskWriteLockScopeKind::Workspace => "workspace",
        TaskWriteLockScopeKind::Path => "path",
    }
}

pub fn task_write_lock_scope_kind_from_db(value: &str) -> Option<TaskWriteLockScopeKind> {
    match value {
        "workspace" => Some(TaskWriteLockScopeKind::Workspace),
        "path" => Some(TaskWriteLockScopeKind::Path),
        _ => None,
    }
}

pub fn task_write_lock_status_to_db(status: TaskWriteLockStatus) -> &'static str {
    match status {
        TaskWriteLockStatus::Pending => "pending",
        TaskWriteLockStatus::Acquired => "acquired",
        TaskWriteLockStatus::Released => "released",
        TaskWriteLockStatus::Expired => "expired",
        TaskWriteLockStatus::Cancelled => "cancelled",
        TaskWriteLockStatus::Blocked => "blocked",
    }
}

pub fn task_write_lock_status_from_db(value: &str) -> Option<TaskWriteLockStatus> {
    match value {
        "pending" => Some(TaskWriteLockStatus::Pending),
        "acquired" => Some(TaskWriteLockStatus::Acquired),
        "released" => Some(TaskWriteLockStatus::Released),
        "expired" => Some(TaskWriteLockStatus::Expired),
        "cancelled" => Some(TaskWriteLockStatus::Cancelled),
        "blocked" => Some(TaskWriteLockStatus::Blocked),
        _ => None,
    }
}

pub fn is_terminal_task_run_status_db(status: &str) -> bool {
    task_run_status_from_db(status).is_some_and(TaskRunStatus::is_terminal)
}
