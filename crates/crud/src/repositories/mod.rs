pub mod agent_memory;
pub mod agent_memory_candidate;
pub mod agent_memory_capsule;
pub mod agent_memory_event;
pub mod agent_memory_policy_decision;
pub mod agent_memory_quality_decision;
pub mod agent_memory_quarantine;
pub mod agent_memory_repair_job;
pub mod artifact;
pub mod cli_runtime_binding;
pub mod hook_run;
pub mod mcp_audit_event;
pub mod mcp_server_catalog_snapshot;
pub mod mcp_server_installation;
pub mod policy;
pub mod recovery_job;
pub mod skill_audit_event;
pub mod skill_dependency_snapshot;
pub mod skill_installation;
pub mod skill_upload_session;
pub mod skill_workspace_policy;
pub mod task;
pub mod task_agent_spec;
pub mod task_delivery;
pub mod task_dependency;
pub mod task_event;
pub mod task_result_candidate;
pub mod task_result_review_event;
pub mod task_run;
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
pub mod turn_cli_runtime_instruction;
pub mod turn_event;
pub mod turn_event_projection_state;
pub mod turn_execution_window;
pub mod turn_item_attempt;
pub mod turn_liveness;
pub mod turn_llm_context;
pub mod turn_mcp_binding;
pub mod turn_mcp_projection;
pub mod turn_runtime_snapshot;
pub mod turn_skill_binding;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionWriteOutcome {
    Applied,
    NoopAlreadyStarted,
    NoopAlreadyTerminal,
    NoopDuplicateTerminal,
    InvariantViolation { reason: String },
}
