use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table("turn_event_projection_stream_state")
                    .if_not_exists()
                    .col(string("turn_id").string_len(21).primary_key())
                    .col(string("thread_id").string_len(21))
                    .col(string("status").string_len(32))
                    .col(string("blocking_event_id").string_len(21).null())
                    .col(text("last_error").null())
                    .col(timestamp_with_time_zone("quarantined_at").null())
                    .col(timestamp_with_time_zone("restored_at").null())
                    .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                    .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_turn_event_projection_stream_status")
                    .table("turn_event_projection_stream_state")
                    .col("status")
                    .col("updated_at")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_turn_event_projection_stream_blocker")
                    .table("turn_event_projection_stream_state")
                    .col("blocking_event_id")
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for index_name in [
            "idx_turn_event_projection_stream_blocker",
            "idx_turn_event_projection_stream_status",
        ] {
            manager
                .drop_index(
                    Index::drop()
                        .if_exists()
                        .name(index_name)
                        .table("turn_event_projection_stream_state")
                        .to_owned(),
                )
                .await?;
        }

        manager
            .drop_table(
                Table::drop()
                    .if_exists()
                    .table("turn_event_projection_stream_state")
                    .to_owned(),
            )
            .await
    }
}
