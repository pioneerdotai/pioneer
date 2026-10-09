//! Additive metadata only; no payload scan or operational activation.
use sea_orm_migration::prelude::*;
#[derive(DeriveMigrationName)]
pub struct Migration;
#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for sql in include_str!("m20261008_000001_frozen_history_proofs.sql").split("-- statement")
        {
            manager
                .get_connection()
                .execute_unprepared(sql.trim())
                .await?;
        }
        Ok(())
    }
    async fn down(&self, _: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Migration(
            "frozen proofs and classified roots cannot be discarded safely".into(),
        ))
    }
}
