pub use sea_orm_migration::prelude::*;

pub struct Migrator;

mod m20260313_125253_create_workspace_table;
mod m20260510_000001_add_hook_run_resume_state;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20260313_125253_create_workspace_table::Migration),
            Box::new(m20260510_000001_add_hook_run_resume_state::Migration),
        ]
    }
}
