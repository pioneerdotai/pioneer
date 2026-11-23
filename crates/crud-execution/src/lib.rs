//! Persistence for execution and recovery.

pub mod agent_action_commit;
pub mod agent_action_compaction;
pub mod agent_action_timeline;
pub mod agent_actions;
pub mod agent_delegation_route;
pub mod agent_execution;
pub mod agent_grant;
pub mod agent_graph_admission;
pub mod agent_graph_cancellation;
pub mod agent_graph_projection;
pub mod agent_graph_resources;
pub mod agent_identity;
pub mod agent_identity_catalog;
pub mod agent_queue;
pub mod agent_thread_creation;
pub mod attempt_supervision;
pub mod attempt_values;
pub mod author_snapshot;
pub mod authority;
pub mod blocked_recovery_resume;
pub mod canonical_id;
pub mod cli_attempt_commits;
pub mod cli_mcp_completion;
pub mod cli_mcp_item;
pub mod cli_runtime_attempts;
pub mod cli_runtime_binding;
pub mod cli_runtime_store;
pub mod execution_admission_lease;
pub mod execution_heartbeat;
pub mod execution_reads;
pub mod hook_run;
pub mod hook_run_store;
pub mod input_mapping;
pub mod native_cancellation_context;
pub mod native_event_cleanup;
pub mod native_events;
pub mod native_execution_window;
pub mod native_terminal_effect_outbox;
pub mod outbox_claim;
pub mod outbox_discovery;
pub mod outbox_retention;
pub mod pending_requests;
pub mod recovery_diagnostic;
pub mod recovery_job;
pub mod recovery_reads;
pub mod recovery_store;
pub mod recovery_terminalization_outbox;
pub mod recovery_terminalization_plan;
pub mod recovery_values;
pub mod recovery_watchdog;
mod retry;
pub mod time;
pub mod timeout_recovery;
pub mod tool_output;
pub mod turn_admission;
pub mod turn_cli_runtime_instruction;
pub mod turn_envelope;
pub mod turn_execution;
pub mod turn_execution_window;
pub mod turn_item_attempt;
pub mod turn_item_terminal;
pub mod turn_ownership;

#[cfg(test)]
mod test_support;

pub mod execution_observability;
pub mod provider_usage;
pub mod turn_runtime_snapshot;

pub mod gateway_control;

pub mod event_append;
pub mod event_liveness;
pub mod event_storage_validation;

pub mod outbox_work;

pub mod terminal_cleanup;
pub mod terminal_effect_store;

pub mod read_model_repair;

#[cfg(test)]
mod test_fixture;
