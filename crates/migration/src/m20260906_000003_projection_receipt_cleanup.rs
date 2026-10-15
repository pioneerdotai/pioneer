use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Schema only. Receipt deletion belongs to cooperative maintenance.
        manager
            .alter_table(
                Table::alter()
                    .table("turn_event_projection_stream_state")
                    .add_column(big_integer("receipts_compacted_through_sequence").default(0))
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Custom(
            "projection receipt compaction cannot be downgraded without restoring receipts".into(),
        ))
    }
}
