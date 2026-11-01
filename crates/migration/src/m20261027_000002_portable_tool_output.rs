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
            "tool_output_chunk",
            "id",
            &["text", "metadata"],
            &["id", "turn_id", "item_id", "stream"],
        )
        .await
    }
    async fn down(&self, m: &SchemaManager) -> Result<(), DbErr> {
        super::portable_payload_target::down(m, "tool_output_chunk", &["text", "metadata"]).await
    }
}
