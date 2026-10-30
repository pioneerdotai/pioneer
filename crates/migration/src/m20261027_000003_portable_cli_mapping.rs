//! Explicit storage reference and indexed pending discovery; payload conversion
//! remains in background maintenance. Existing verified-root triggers remain.
use sea_orm_migration::prelude::*;
#[derive(DeriveMigrationName)]
pub struct Migration;
#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
    async fn up(&self, m: &SchemaManager) -> Result<(), DbErr> {
        super::portable_payload_target::up(
            m,
            "turn_cli_runtime_binding",
            "turn_id",
            &["input_mapping_json"],
            &[
                "turn_id",
                "thread_id",
                "workspace_id",
                "continuation_thread_id",
                "frozen_manifest_id",
            ],
        )
        .await
    }
    async fn down(&self, m: &SchemaManager) -> Result<(), DbErr> {
        super::portable_payload_target::down(m, "turn_cli_runtime_binding", &["input_mapping_json"])
            .await
    }
}
