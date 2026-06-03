use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table("turn_execution_window")
                    .if_not_exists()
                    .col(string("id").string_len(21).primary_key())
                    .col(string("workspace_id").string_len(21))
                    .col(string("thread_id").string_len(21))
                    .col(string("turn_id").string_len(21))
                    .col(integer("window_index"))
                    .col(string("status").string_len(32))
                    .col(string("exhaustion_reason").string_len(64).null())
                    .col(integer("agent_round_count").default(0))
                    .col(integer("tool_call_count").default(0))
                    .col(integer("provider_token_count").default(0))
                    .col(text("metadata_json").default("{}"))
                    .col(timestamp_with_time_zone("started_at"))
                    .col(timestamp_with_time_zone("completed_at").null())
                    .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                    .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("uidx_turn_execution_window_turn_index")
                    .table("turn_execution_window")
                    .col("turn_id")
                    .col("window_index")
                    .unique()
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_turn_execution_window_turn_id")
                    .table("turn_execution_window")
                    .col("turn_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_turn_execution_window_thread_turn")
                    .table("turn_execution_window")
                    .col("thread_id")
                    .col("turn_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_turn_execution_window_status")
                    .table("turn_execution_window")
                    .col("status")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("turn_execution_checkpoint")
                    .if_not_exists()
                    .col(string("id").string_len(21).primary_key())
                    .col(string("window_id").string_len(21))
                    .col(string("workspace_id").string_len(21))
                    .col(string("thread_id").string_len(21))
                    .col(string("turn_id").string_len(21))
                    .col(string("checkpoint_kind").string_len(64))
                    .col(text("payload_json"))
                    .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_turn_execution_checkpoint_window")
                    .table("turn_execution_checkpoint")
                    .col("window_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_turn_execution_checkpoint_turn")
                    .table("turn_execution_checkpoint")
                    .col("turn_id")
                    .col("created_at")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_turn_execution_checkpoint_thread_turn")
                    .table("turn_execution_checkpoint")
                    .col("thread_id")
                    .col("turn_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_turn_execution_checkpoint_kind")
                    .table("turn_execution_checkpoint")
                    .col("checkpoint_kind")
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for index_name in [
            "idx_turn_execution_checkpoint_kind",
            "idx_turn_execution_checkpoint_thread_turn",
            "idx_turn_execution_checkpoint_turn",
            "idx_turn_execution_checkpoint_window",
        ] {
            manager
                .drop_index(
                    Index::drop()
                        .if_exists()
                        .name(index_name)
                        .table("turn_execution_checkpoint")
                        .to_owned(),
                )
                .await?;
        }

        manager
            .drop_table(
                Table::drop()
                    .if_exists()
                    .table("turn_execution_checkpoint")
                    .to_owned(),
            )
            .await?;

        for index_name in [
            "idx_turn_execution_window_status",
            "idx_turn_execution_window_thread_turn",
            "idx_turn_execution_window_turn_id",
            "uidx_turn_execution_window_turn_index",
        ] {
            manager
                .drop_index(
                    Index::drop()
                        .if_exists()
                        .name(index_name)
                        .table("turn_execution_window")
                        .to_owned(),
                )
                .await?;
        }

        manager
            .drop_table(
                Table::drop()
                    .if_exists()
                    .table("turn_execution_window")
                    .to_owned(),
            )
            .await
    }
}
