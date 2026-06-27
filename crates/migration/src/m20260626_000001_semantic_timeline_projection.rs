use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table("thread_timeline_projection_meta")
                    .if_not_exists()
                    .col(string("projection_key").string_len(64).primary_key())
                    .col(integer("projection_version"))
                    .col(string("status").string_len(32))
                    .col(big_integer("source_thread_count").default(0))
                    .col(big_integer("source_turn_count").default(0))
                    .col(big_integer("source_turn_item_count").default(0))
                    .col(big_integer("source_turn_event_count").default(0))
                    .col(text("last_error").null())
                    .col(timestamp_with_time_zone("backfill_started_at").null())
                    .col(timestamp_with_time_zone("backfilled_at").null())
                    .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                    .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("thread_timeline_block")
                    .if_not_exists()
                    .col(string("block_id").string_len(128).primary_key())
                    .col(string("workspace_id").string_len(21))
                    .col(string("thread_id").string_len(21))
                    .col(string("turn_id").string_len(21).null())
                    .col(string("block_kind").string_len(32))
                    .col(string("sort_key").string_len(128))
                    .col(string("source_kind").string_len(32).null())
                    .col(string("source_key").string_len(128).null())
                    .col(timestamp_with_time_zone("started_at").null())
                    .col(timestamp_with_time_zone("completed_at").null())
                    .col(text("metadata_json").default("{}"))
                    .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                    .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_thread_timeline_block_thread_sort")
                    .table("thread_timeline_block")
                    .col("thread_id")
                    .col("sort_key")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_thread_timeline_block_thread_kind")
                    .table("thread_timeline_block")
                    .col("thread_id")
                    .col("block_kind")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_thread_timeline_block_turn")
                    .table("thread_timeline_block")
                    .col("turn_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("turn_work_projection")
                    .if_not_exists()
                    .col(string("turn_id").string_len(21).primary_key())
                    .col(string("workspace_id").string_len(21))
                    .col(string("thread_id").string_len(21))
                    .col(string("presentation").string_len(32))
                    .col(string("state").string_len(32))
                    .col(big_integer("work_count").default(0))
                    .col(big_integer("visible_work_count").default(0))
                    .col(big_integer("hidden_work_count").default(0))
                    .col(string("first_work_item_id").string_len(128).null())
                    .col(string("last_work_item_id").string_len(128).null())
                    .col(timestamp_with_time_zone("started_at").null())
                    .col(timestamp_with_time_zone("completed_at").null())
                    .col(big_integer("elapsed_ms").null())
                    .col(big_integer("source_high_watermark").default(0))
                    .col(text("metadata_json").default("{}"))
                    .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                    .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_turn_work_projection_thread")
                    .table("turn_work_projection")
                    .col("thread_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("turn_work_item_projection")
                    .if_not_exists()
                    .col(string("work_item_id").string_len(128).primary_key())
                    .col(string("workspace_id").string_len(21))
                    .col(string("thread_id").string_len(21))
                    .col(string("turn_id").string_len(21))
                    .col(string("item_id").string_len(128))
                    .col(string("source_event_id").string_len(21).null())
                    .col(big_integer("source_sequence").default(0))
                    .col(string("order_key").string_len(128))
                    .col(string("item_type").string_len(64))
                    .col(string("visibility").string_len(16))
                    .col(string("classification").string_len(64))
                    .col(string("status").string_len(32))
                    .col(timestamp_with_time_zone("started_at").null())
                    .col(timestamp_with_time_zone("completed_at").null())
                    .col(text("metadata_json").default("{}"))
                    .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                    .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("uq_turn_work_item_projection_turn_item")
                    .table("turn_work_item_projection")
                    .col("turn_id")
                    .col("item_id")
                    .unique()
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_turn_work_item_projection_turn_order")
                    .table("turn_work_item_projection")
                    .col("turn_id")
                    .col("order_key")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_turn_work_item_projection_turn_visibility")
                    .table("turn_work_item_projection")
                    .col("turn_id")
                    .col("visibility")
                    .col("order_key")
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for (table, indexes) in [
            (
                "turn_work_item_projection",
                &[
                    "idx_turn_work_item_projection_turn_visibility",
                    "idx_turn_work_item_projection_turn_order",
                    "uq_turn_work_item_projection_turn_item",
                ][..],
            ),
            (
                "turn_work_projection",
                &["idx_turn_work_projection_thread"][..],
            ),
            (
                "thread_timeline_block",
                &[
                    "idx_thread_timeline_block_turn",
                    "idx_thread_timeline_block_thread_kind",
                    "idx_thread_timeline_block_thread_sort",
                ][..],
            ),
        ] {
            for index_name in indexes {
                manager
                    .drop_index(
                        Index::drop()
                            .if_exists()
                            .name(*index_name)
                            .table(table)
                            .to_owned(),
                    )
                    .await?;
            }
        }

        for table in [
            "turn_work_item_projection",
            "turn_work_projection",
            "thread_timeline_block",
            "thread_timeline_projection_meta",
        ] {
            manager
                .drop_table(Table::drop().if_exists().table(table).to_owned())
                .await?;
        }

        Ok(())
    }
}
