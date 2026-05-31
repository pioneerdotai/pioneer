pub use sea_orm_migration::prelude::*;

pub struct Migrator;

mod m20260313_125253_create_workspace_table;
mod m20260510_000001_add_hook_run_resume_state;
mod m20260515_000001_unique_thread_lineage_task_run;
mod m20260517_000001_workspace_single_current;
mod m20260523_000001_turn_mcp_binding_metadata;
mod m20260531_000001_task_review;

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
        ]
    }
}
