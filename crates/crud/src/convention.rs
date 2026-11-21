pub use pioneer_crud_access::turn_execution::{
    turn_kind_from_db, turn_origin_from_db, turn_status_from_db,
};

use anyhow::Result;
use pioneer_protocol::{
    MemoryActorKind, MemoryCandidateStatus, MemoryCategory, MemoryEvidenceClass, MemoryFactClass,
    MemoryLifecycleActorKind, MemoryLifecycleReasonCode, MemoryLifetimeClass, MemoryOwnershipClass,
    MemoryQualityAction, MemoryScopeKind, MemorySensitivity, MemorySourceContextKind, MemoryStatus,
    MemoryWriteRelation, TaskExecutorKind, TaskRunExecutionStatus, TaskTriggerKind,
    TaskTriggerStatus, ThreadMode, TurnItem, TurnStatus,
};
use serde::{Serialize, de::DeserializeOwned};

pub const DB_ID_LEN: usize = 21;
pub use pioneer_crud_execution::attempt_values::*;

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

pub use pioneer_crud_access::policy::sandbox_mode_to_db;

pub use pioneer_crud_conversations::values::{
    thread_origin_kind_from_db, thread_origin_kind_to_db, thread_sidebar_visibility_from_db,
    thread_sidebar_visibility_to_db, thread_status_from_db, thread_status_to_db,
};

pub use pioneer_crud_conversations::collaboration::{
    PersistedTurnSendMode, canonical_turn_mentions_json, turn_mentions_from_db,
    turn_send_mode_from_db, turn_send_mode_to_db, validate_turn_author_snapshot,
};

pub use pioneer_crud_tasks::status::{
    is_terminal_task_status, is_terminal_task_status_db, task_status_from_db, task_status_to_db,
};

pub use pioneer_crud_tasks::status::{task_run_status_from_db, task_run_status_to_db};

pub use pioneer_crud_tasks::status::{
    is_terminal_task_run_status, task_executor_kind_from_db, task_executor_kind_to_db,
    task_trigger_kind_from_db, task_trigger_kind_to_db, task_trigger_status_from_db,
    task_trigger_status_to_db,
};

pub use pioneer_crud_tasks::status::is_terminal_task_run_status_db;

pub use pioneer_crud_tasks::status::{
    is_terminal_task_run_execution_status, task_run_execution_status_from_db,
    task_run_execution_status_to_db,
};

pub use pioneer_crud_tasks::status::{
    task_concurrency_conflict_policy_from_db, task_concurrency_conflict_policy_to_db,
    task_result_candidate_status_from_db, task_result_candidate_status_to_db,
    task_result_review_decision_from_db, task_result_review_decision_to_db,
    task_result_review_event_kind_from_db, task_result_review_event_kind_to_db,
    task_result_reviewer_kind_from_db, task_result_reviewer_kind_to_db,
    task_run_thread_binding_kind_from_db, task_run_thread_binding_kind_to_db,
    task_run_turn_kind_from_db, task_run_turn_kind_to_db, task_run_turn_status_from_db,
    task_run_turn_status_to_db, task_write_lock_scope_kind_from_db,
    task_write_lock_scope_kind_to_db, task_write_lock_status_from_db, task_write_lock_status_to_db,
};

pub use pioneer_crud_tasks::status::{task_owner_kind_from_db, task_owner_kind_to_db};

pub use pioneer_crud_conversations::values::{turn_kind_to_db, turn_origin_to_db};

pub use pioneer_crud_conversations::turn_writes::{
    turn_permission_mode_from_db, turn_permission_mode_to_db,
    turn_permission_profile_source_from_db, turn_permission_profile_source_to_db,
};

pub fn turn_item_id_and_type_to_db(item: &TurnItem) -> (&str, &'static str) {
    (item.item_id(), turn_item_type_to_db(item.item_type()))
}

pub use pioneer_crud_conversations::turn_item::{turn_item_type_from_db, turn_item_type_to_db};

pub use pioneer_crud_execution::recovery_values::{
    RECOVERY_STATUS_ACTIVE, RECOVERY_STATUS_BLOCKED, RECOVERY_STATUS_CANCELLED,
    RECOVERY_STATUS_EXHAUSTED, RECOVERY_STATUS_FAILED, RECOVERY_STATUS_PENDING,
    RECOVERY_STATUS_SUCCEEDED, provider_failure_class_from_db, provider_failure_class_to_db,
    provider_failure_stage_from_db, provider_failure_stage_to_db, recovery_action_from_db,
    recovery_action_to_db, recovery_job_status_from_db, recovery_job_status_to_db,
    recovery_trigger_from_db, recovery_trigger_to_db,
};

#[cfg(test)]
mod tests {
    use super::{
        recovery_job_status_to_db, sandbox_mode_to_db, turn_status_from_db, turn_status_to_db,
    };
    use pioneer_protocol::{RecoveryJobStatus, SandboxMode, TurnStatus};

    #[test]
    fn db_values_are_snake_case() {
        assert_eq!(sandbox_mode_to_db(SandboxMode::FullAccess), "full_access");
        assert_eq!(turn_status_to_db(TurnStatus::InProgress), "in_progress");
        assert_eq!(turn_status_to_db(TurnStatus::Completed), "completed");
        assert_eq!(turn_status_to_db(TurnStatus::Failed), "failed");
        assert_eq!(turn_status_to_db(TurnStatus::Interrupted), "interrupted");
        assert_eq!(turn_status_to_db(TurnStatus::Blocked), "blocked");
        assert_eq!(
            recovery_job_status_to_db(RecoveryJobStatus::Blocked),
            "blocked"
        );
    }

    #[test]
    fn turn_status_parser_accepts_snake_case_only() {
        assert_eq!(
            turn_status_from_db("in_progress"),
            Some(TurnStatus::InProgress)
        );
        assert_eq!(
            turn_status_from_db("completed"),
            Some(TurnStatus::Completed)
        );
        assert_eq!(turn_status_from_db("failed"), Some(TurnStatus::Failed));
        assert_eq!(
            turn_status_from_db("interrupted"),
            Some(TurnStatus::Interrupted)
        );
        assert_eq!(turn_status_from_db("blocked"), Some(TurnStatus::Blocked));
        assert_eq!(turn_status_from_db("InProgress"), None);
    }
}

pub use pioneer_crud_tasks::delivery::{
    task_delivery_attempt_status_from_db, task_delivery_attempt_status_to_db,
    task_delivery_mode_from_db, task_delivery_mode_to_db, task_delivery_status_from_db,
    task_delivery_status_to_db, task_delivery_thread_target_from_db,
    task_delivery_thread_target_to_db,
};

pub use pioneer_crud_conversations::values::{
    thread_mode_from_db, thread_mode_to_db, turn_status_to_db,
};
