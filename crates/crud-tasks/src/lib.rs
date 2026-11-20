//! Task repositories that participate in the caller's database transaction.

pub mod completion_inbox;
pub mod conversation_snapshot;
pub mod delivery;
pub mod delivery_recovery;
pub mod dispatch;
pub mod event_fanout;
pub mod event_log;
pub mod occurrence_reconcile;
pub mod occurrence_scan;
pub mod status;
pub mod task_cancellation_actor;
pub mod user_notification_outbox;
pub mod webhook_run;
pub mod write_lock;

pub mod child_work;
pub mod task_child_cleanup;
pub mod task_child_producers;
pub mod task_occurrence_producers;

pub mod projection;
pub mod task_agent_spec;
pub mod task_dependency;
pub mod task_run;
pub mod task_run_execution;
pub mod task_trigger;
pub mod values;
