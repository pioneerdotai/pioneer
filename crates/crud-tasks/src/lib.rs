//! Task repositories that participate in the caller's database transaction.

pub mod completion_inbox;
pub mod conversation_snapshot;
pub mod delivery;
pub mod delivery_recovery;
pub mod dispatch;
pub mod event_fanout;
pub mod event_log;
pub mod event_reads;
pub mod event_records;
pub mod occurrence_reconcile;
pub mod occurrence_scan;
pub mod status;
pub mod task_cancellation_actor;
pub mod user_notification_outbox;
pub mod webhook_admission;
pub mod webhook_run;
pub mod write_lock;

pub mod child_scopes;
pub mod child_work;
pub mod task_child_cleanup;
pub mod task_child_producers;
pub mod task_occurrence_producers;
pub mod task_occurrence_reconcile;

pub mod projection;
pub mod review_invariants;
pub mod runtime_invariants;
pub mod task;
pub mod task_actor_contract;
pub mod task_agent_spec;
pub mod task_dependency;
pub mod task_event;
pub mod task_execution_admission;
pub mod task_projector;
pub mod task_run;
pub mod task_run_execution;
pub mod task_trigger;
pub mod task_views;
pub mod values;

pub mod task_result_candidate;
pub mod task_result_review_event;
pub mod task_run_thread_binding;
pub mod task_run_turn;

#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub mod test_support {
    #[cfg(feature = "test-support")]
    pub mod completion_inbox;
    pub mod occurrence;
    pub mod task_projection;
}
