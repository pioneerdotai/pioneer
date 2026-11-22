pub use sea_orm_migration::prelude::*;

mod m20261119_000001_identity;
mod m20261119_000002_packages;
mod m20261119_000003_payloads;
mod m20261119_000004_conversations;
mod m20261119_000005_execution;
mod m20261119_000006_tasks;
mod m20261119_000007_history_storage;
mod m20261119_000008_memory;
mod m20261119_000009_relations;
mod m20261119_000010_database_rules;
mod m20261122_000011_history_epoch_lock_order;
mod m20261122_000012_package_component_lock_order;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20261119_000001_identity::Migration),
            Box::new(m20261119_000002_packages::Migration),
            Box::new(m20261119_000003_payloads::Migration),
            Box::new(m20261119_000004_conversations::Migration),
            Box::new(m20261119_000005_execution::Migration),
            Box::new(m20261119_000006_tasks::Migration),
            Box::new(m20261119_000007_history_storage::Migration),
            Box::new(m20261119_000008_memory::Migration),
            Box::new(m20261119_000009_relations::Migration),
            Box::new(m20261119_000010_database_rules::Migration),
            Box::new(m20261122_000011_history_epoch_lock_order::Migration),
            Box::new(m20261122_000012_package_component_lock_order::Migration),
        ]
    }
}

async fn drop_tables(manager: &SchemaManager<'_>, names: &[&str]) -> Result<(), DbErr> {
    for &name in names {
        manager
            .drop_table(Table::drop().table(Alias::new(name)).to_owned())
            .await?;
    }
    Ok(())
}
