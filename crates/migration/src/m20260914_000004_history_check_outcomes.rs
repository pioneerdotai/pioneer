//! Metadata only. Legacy failed jobs are reconciled by bounded runtime work.
use sea_orm_migration::{prelude::*, schema::*};
#[derive(DeriveMigrationName)]
pub struct Migration;
#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
    async fn up(&self, m: &SchemaManager) -> Result<(), DbErr> {
        for column in [
            big_integer("revision").default(0),
            big_integer("failures").default(0),
            big_integer("next_attempt_ms").default(0),
            big_integer("attempt_deadline_ms").null(),
            text("diagnostic").null(),
            text("config_hash").null(),
            big_integer("managed").default(0),
        ] {
            m.alter_table(
                Table::alter()
                    .table(Alias::new("compaction_history_check"))
                    .add_column(column)
                    .to_owned(),
            )
            .await?;
        }
        m.create_index(
            Index::create()
                .name("history_check_due")
                .table(Alias::new("compaction_history_check"))
                .col("state")
                .col("next_attempt_ms")
                .col("turn_id")
                .to_owned(),
        )
        .await?;
        m.create_index(
            Index::create()
                .name("history_check_legacy")
                .table(Alias::new("compaction_history_check"))
                .col("state")
                .col("outcome")
                .col("managed")
                .col("next_attempt_ms")
                .to_owned(),
        )
        .await?;
        Ok(())
    }
    async fn down(&self, _m: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Custom(
            "history check downgrade requires draining durable retries".into(),
        ))
    }
}
