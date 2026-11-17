//! Physical Task event storage. The pending index scans existing keys once;
//! historical conversion remains bounded background work. Existing INSERT-only
//! fanout triggers, sequence allocation and idempotency constraints are unchanged.
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
            "task_event",
            "id",
            &["payload_json"],
            &[
                "id",
                "task_id",
                "run_id",
                "thread_id",
                "turn_id",
                "sequence",
                "event_type",
                "idempotency_key",
                "created_at",
            ],
        )
        .await
    }
    async fn down(&self, m: &SchemaManager) -> Result<(), DbErr> {
        super::portable_payload_target::down(m, "task_event", &["payload_json"]).await
    }
}
