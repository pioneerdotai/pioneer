pub use sea_orm_migration::prelude::*;

pub struct Migrator;

mod m20260313_125253_create_workspace_table;
mod m20260510_000001_add_hook_run_resume_state;
mod m20260515_000001_unique_thread_lineage_task_run;
mod m20260517_000001_workspace_single_current;
mod m20260523_000001_turn_mcp_binding_metadata;
mod m20260531_000001_task_review;
mod m20260531_000002_remove_old_task_execution_fields;
mod m20260531_000003_task_agent_review_policy;
mod m20260603_000001_turn_execution_window;
mod m20260613_000001_turn_event_projection_state;
mod m20260614_000001_thread_episodic_memory;
mod m20260616_000001_cli_runtime_bindings;
mod m20260624_000001_turn_reasoning_effort;
mod m20260626_000001_semantic_timeline_projection;
mod m20260628_000001_turn_permission_profile;
mod m20260701_000001_turn_liveness;
mod m20260704_000001_backfill_task_agent_permission_cap;
mod m20260704_000002_thread_episodic_items_no_chunks;
mod m20260706_000001_turn_execution_security_snapshot;
mod m20260707_000001_projection_meta_config;
mod m20260714_000001_cli_runtime_turn_attempt;
mod m20260715_000001_cli_runtime_recovery_confirmation;
mod m20260715_000002_turn_mcp_projection;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20260313_125253_create_workspace_table::Migration),
            Box::new(m20260510_000001_add_hook_run_resume_state::Migration),
            Box::new(m20260515_000001_unique_thread_lineage_task_run::Migration),
            Box::new(m20260517_000001_workspace_single_current::Migration),
            Box::new(m20260523_000001_turn_mcp_binding_metadata::Migration),
            Box::new(m20260531_000001_task_review::Migration),
            Box::new(m20260531_000002_remove_old_task_execution_fields::Migration),
            Box::new(m20260531_000003_task_agent_review_policy::Migration),
            Box::new(m20260603_000001_turn_execution_window::Migration),
            Box::new(m20260613_000001_turn_event_projection_state::Migration),
            Box::new(m20260614_000001_thread_episodic_memory::Migration),
            Box::new(m20260616_000001_cli_runtime_bindings::Migration),
            Box::new(m20260624_000001_turn_reasoning_effort::Migration),
            Box::new(m20260626_000001_semantic_timeline_projection::Migration),
            Box::new(m20260628_000001_turn_permission_profile::Migration),
            Box::new(m20260701_000001_turn_liveness::Migration),
            Box::new(m20260704_000001_backfill_task_agent_permission_cap::Migration),
            Box::new(m20260704_000002_thread_episodic_items_no_chunks::Migration),
            Box::new(m20260706_000001_turn_execution_security_snapshot::Migration),
            Box::new(m20260707_000001_projection_meta_config::Migration),
            Box::new(m20260714_000001_cli_runtime_turn_attempt::Migration),
            Box::new(m20260715_000002_turn_mcp_projection::Migration),
            Box::new(m20260715_000001_cli_runtime_recovery_confirmation::Migration),
        ]
    }
}
