use pioneer_protocol::{
    TaskExecutorKind, TaskRunExecutionStatus, TaskRunStatus, TaskTriggerKind, TaskTriggerStatus,
};

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
