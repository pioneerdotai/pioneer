pub mod administrative_audit;
pub mod agent_domain;
pub mod agent_identity_catalog;
pub mod agent_memory;
pub mod agent_memory_candidate;
pub mod agent_memory_capsule;
pub mod agent_memory_event;
pub mod agent_memory_policy_decision;
pub mod agent_memory_quality_decision;
pub mod agent_memory_quarantine;
pub mod agent_memory_repair_job;
pub(crate) mod agent_skill;
pub mod artifact;
pub(crate) mod auth_session;
pub(crate) mod authorization_persistence;
pub(crate) mod authorization_scope;
pub(crate) mod canonical_turn_event;
pub mod cli_runtime_binding;
pub mod execution_admission_lease;
pub mod hook_run;
pub(crate) mod identity;
pub(crate) mod invitation;
pub mod mcp_audit_event;
pub mod mcp_server_catalog_snapshot;
pub mod mcp_server_installation;
pub(crate) mod membership;
pub mod patch_history;
pub mod policy;
pub(crate) mod policy_generation;
pub(crate) mod principal_avatar;
pub(crate) mod read_model_repair;
pub mod recovery_job;
pub mod recovery_terminalization_outbox;
pub(crate) mod self_improvement_finalization;
pub(crate) mod self_improvement_run;
pub(crate) mod self_improvement_source_turn;
pub(crate) mod self_improvement_workspace_state;
pub mod skill_audit_event;
pub mod skill_dependency_snapshot;
pub mod skill_installation;
pub mod skill_pack_installation;
pub mod skill_upload_session;
pub mod skill_workspace_policy;
pub mod task;
pub mod task_actor_contract;
pub mod task_agent_spec;
pub mod task_delivery;
pub mod task_dependency;
pub mod task_event;
pub(crate) mod task_execution_admission;
pub mod task_result_candidate;
pub mod task_result_review_event;
pub mod task_run;
pub mod task_run_conversation_snapshot;
pub mod task_run_execution;
pub mod task_run_thread_binding;
pub mod task_run_turn;
pub mod task_trigger;
pub mod task_write_lock;
pub mod thread;
pub mod thread_agents_doc;
pub mod thread_episodic;
pub mod thread_lineage;
pub mod thread_timeline_projection;
pub mod thread_tree;
pub mod turn;
pub mod turn_admission;
pub mod turn_cli_runtime_instruction;
pub mod turn_event;
pub mod turn_event_delivery;
pub mod turn_event_projection_state;
pub mod turn_event_projection_stream_state;
pub mod turn_execution;
pub mod turn_execution_window;
pub mod turn_finalization;
pub mod turn_item_attempt;
pub mod turn_liveness;
pub mod turn_llm_context;
pub mod turn_mcp_binding;
pub mod turn_mcp_projection;
pub mod turn_runtime_snapshot;
pub mod turn_skill_binding;
pub(crate) mod user_notification_outbox;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionWriteOutcome {
    Applied,
    NoopAlreadyStarted,
    NoopAlreadyTerminal,
    NoopDuplicateTerminal,
    InvariantViolation { reason: String },
}
