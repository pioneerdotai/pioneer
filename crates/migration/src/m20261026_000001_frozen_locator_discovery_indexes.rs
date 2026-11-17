//! Index-only upgrade for bounded unknown-root discovery. Existing scoped
//! frozen_root indexes still serve workspace/Task retention queries.
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

const INDEXES: [(&str, &str, &str); 4] = [
    (
        "task_run_conversation_snapshot_frozen_locator_discovery",
        "task_run_conversation_snapshot",
        "run_id",
    ),
    (
        "turn_runtime_snapshot_frozen_locator_discovery",
        "turn_runtime_snapshot",
        "turn_id",
    ),
    (
        "thread_cli_runtime_binding_frozen_locator_discovery",
        "thread_cli_runtime_binding",
        "thread_id",
    ),
    (
        "turn_cli_runtime_binding_frozen_locator_discovery",
        "turn_cli_runtime_binding",
        "turn_id",
    ),
];

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }

    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for (name, table, key) in INDEXES {
            manager
                .create_index(
                    Index::create()
                        .if_not_exists()
                        .name(name)
                        .table(Alias::new(table))
                        .col(Alias::new("frozen_manifest_id"))
                        .col(Alias::new(key))
                        .to_owned(),
                )
                .await?;
        }
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for (name, table, _) in INDEXES {
            manager
                .drop_index(
                    Index::drop()
                        .if_exists()
                        .name(name)
                        .table(Alias::new(table))
                        .to_owned(),
                )
                .await?;
        }
        Ok(())
    }
}
