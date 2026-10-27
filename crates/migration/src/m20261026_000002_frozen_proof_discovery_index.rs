use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }

    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Full equality/keyset prefix also serves parameterized proof_version=0
        // discovery. Operation- and owner-leading indexes serve other callers.
        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("checkpoint_proof_discovery")
                    .table(Alias::new("compaction_checkpoint"))
                    .col("proof_version")
                    .col("id")
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(
                Index::drop()
                    .if_exists()
                    .name("checkpoint_proof_discovery")
                    .table(Alias::new("compaction_checkpoint"))
                    .to_owned(),
            )
            .await
    }
}
