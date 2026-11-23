use pioneer_protocol::{TurnItemAttemptStatus, TurnItemExecutionClass, TurnItemTimeoutReason};

pub const TURN_ITEM_STATUS_IN_PROGRESS: &str = "in_progress";
pub const TURN_ITEM_STATUS_COMPLETED: &str = "completed";
pub const TURN_ITEM_STATUS_FAILED: &str = "failed";
pub const TURN_ITEM_STATUS_TIMED_OUT: &str = "timed_out";
pub const TURN_ITEM_STATUS_CANCELLED: &str = "cancelled";

pub const ATTEMPT_STATUS_RUNNING: &str = "running";
pub const ATTEMPT_STATUS_COMPLETED: &str = "completed";
pub const ATTEMPT_STATUS_FAILED: &str = "failed";
pub const ATTEMPT_STATUS_TIMED_OUT: &str = "timed_out";
pub const ATTEMPT_STATUS_CANCELLED: &str = "cancelled";
pub const ATTEMPT_STATUS_INTERRUPTED: &str = "interrupted";
pub const ATTEMPT_STATUS_RETRYING: &str = "retrying";
pub const ATTEMPT_STATUS_EXHAUSTED: &str = "exhausted";

pub fn turn_item_execution_class_to_db(execution_class: TurnItemExecutionClass) -> &'static str {
    match execution_class {
        TurnItemExecutionClass::Standard => "standard",
        TurnItemExecutionClass::DurableWait => "durable_wait",
        TurnItemExecutionClass::ContextCompaction => "context_compaction",
    }
}

pub fn turn_item_execution_class_from_db(value: &str) -> Option<TurnItemExecutionClass> {
    match value {
        "standard" => Some(TurnItemExecutionClass::Standard),
        "durable_wait" => Some(TurnItemExecutionClass::DurableWait),
        "context_compaction" => Some(TurnItemExecutionClass::ContextCompaction),
        _ => None,
    }
}

pub fn turn_item_attempt_status_to_db(status: TurnItemAttemptStatus) -> &'static str {
    match status {
        TurnItemAttemptStatus::Running => ATTEMPT_STATUS_RUNNING,
        TurnItemAttemptStatus::Completed => ATTEMPT_STATUS_COMPLETED,
        TurnItemAttemptStatus::Failed => ATTEMPT_STATUS_FAILED,
        TurnItemAttemptStatus::TimedOut => ATTEMPT_STATUS_TIMED_OUT,
        TurnItemAttemptStatus::Cancelled => ATTEMPT_STATUS_CANCELLED,
        TurnItemAttemptStatus::Interrupted => ATTEMPT_STATUS_INTERRUPTED,
        TurnItemAttemptStatus::Retrying => ATTEMPT_STATUS_RETRYING,
        TurnItemAttemptStatus::Exhausted => ATTEMPT_STATUS_EXHAUSTED,
    }
}

#[allow(dead_code)]
pub fn turn_item_attempt_status_from_db(value: &str) -> Option<TurnItemAttemptStatus> {
    match value {
        ATTEMPT_STATUS_RUNNING => Some(TurnItemAttemptStatus::Running),
        ATTEMPT_STATUS_COMPLETED => Some(TurnItemAttemptStatus::Completed),
        ATTEMPT_STATUS_FAILED => Some(TurnItemAttemptStatus::Failed),
        ATTEMPT_STATUS_TIMED_OUT => Some(TurnItemAttemptStatus::TimedOut),
        ATTEMPT_STATUS_CANCELLED => Some(TurnItemAttemptStatus::Cancelled),
        ATTEMPT_STATUS_INTERRUPTED => Some(TurnItemAttemptStatus::Interrupted),
        ATTEMPT_STATUS_RETRYING => Some(TurnItemAttemptStatus::Retrying),
        ATTEMPT_STATUS_EXHAUSTED => Some(TurnItemAttemptStatus::Exhausted),
        _ => None,
    }
}

pub fn turn_item_timeout_reason_to_db(reason: TurnItemTimeoutReason) -> &'static str {
    match reason {
        TurnItemTimeoutReason::StartDeadlineExceeded => "start_deadline_exceeded",
        TurnItemTimeoutReason::IdleDeadlineExceeded => "idle_deadline_exceeded",
        TurnItemTimeoutReason::HardDeadlineExceeded => "hard_deadline_exceeded",
        TurnItemTimeoutReason::LeaseExpired => "lease_expired",
    }
}

#[allow(dead_code)]
pub fn turn_item_timeout_reason_from_db(value: &str) -> Option<TurnItemTimeoutReason> {
    match value {
        "start_deadline_exceeded" => Some(TurnItemTimeoutReason::StartDeadlineExceeded),
        "idle_deadline_exceeded" => Some(TurnItemTimeoutReason::IdleDeadlineExceeded),
        "hard_deadline_exceeded" => Some(TurnItemTimeoutReason::HardDeadlineExceeded),
        "lease_expired" => Some(TurnItemTimeoutReason::LeaseExpired),
        _ => None,
    }
}
