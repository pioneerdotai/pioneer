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
        ]
    }
}
