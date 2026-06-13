use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table("turn_event_projection_state")
                    .if_not_exists()
                    .col(string("event_id").string_len(21).primary_key())
                    .col(string("thread_id").string_len(21))
                    .col(string("turn_id").string_len(21))
                    .col(big_integer("sequence"))
                    .col(string("status").string_len(32))
                    .col(integer("attempt_count").default(0))
                    .col(text("last_error").null())
                    .col(timestamp_with_time_zone("next_run_at"))
                    .col(string("claim_token").string_len(21).null())
                    .col(timestamp_with_time_zone("claim_expires_at").null())
                    .col(text("projection_context_json").default("{}"))
                    .col(timestamp_with_time_zone("projected_at").null())
                    .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                    .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_turn_event_projection_due")
                    .table("turn_event_projection_state")
                    .col("status")
                    .col("next_run_at")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_turn_event_projection_claim")
                    .table("turn_event_projection_state")
                    .col("claim_expires_at")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_turn_event_projection_turn")
                    .table("turn_event_projection_state")
                    .col("turn_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_turn_event_projection_turn_sequence")
                    .table("turn_event_projection_state")
                    .col("turn_id")
                    .col("sequence")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_turn_event_projection_thread_turn")
                    .table("turn_event_projection_state")
                    .col("thread_id")
                    .col("turn_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("turn_runtime_snapshot")
                    .if_not_exists()
                    .col(string("turn_id").string_len(21).primary_key())
                    .col(string("thread_id").string_len(21))
                    .col(string("workspace_id").string_len(21))
                    .col(text("mode_json"))
                    .col(text("model"))
                    .col(text("provider_name"))
                    .col(text("hook_runtime_context_json"))
                    .col(text("workspace_skill_policies_json"))
                    .col(text("input_json"))
                    .col(text("capabilities_json"))
                    .col(text("resolved_artifacts_json"))
                    .col(text("runtime_environment_json"))
                    .col(text("history_json"))
                    .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                    .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_turn_runtime_snapshot_thread")
                    .table("turn_runtime_snapshot")
                    .col("thread_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_turn_runtime_snapshot_workspace")
                    .table("turn_runtime_snapshot")
                    .col("workspace_id")
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for index_name in [
            "idx_turn_runtime_snapshot_workspace",
            "idx_turn_runtime_snapshot_thread",
        ] {
            manager
                .drop_index(
                    Index::drop()
                        .if_exists()
                        .name(index_name)
                        .table("turn_runtime_snapshot")
                        .to_owned(),
                )
                .await?;
        }

        manager
            .drop_table(
                Table::drop()
                    .if_exists()
                    .table("turn_runtime_snapshot")
                    .to_owned(),
            )
            .await?;

        for index_name in [
            "idx_turn_event_projection_thread_turn",
            "idx_turn_event_projection_turn_sequence",
            "idx_turn_event_projection_turn",
            "idx_turn_event_projection_claim",
            "idx_turn_event_projection_due",
        ] {
            manager
                .drop_index(
                    Index::drop()
                        .if_exists()
                        .name(index_name)
                        .table("turn_event_projection_state")
                        .to_owned(),
                )
                .await?;
        }

        manager
            .drop_table(
                Table::drop()
                    .if_exists()
                    .table("turn_event_projection_state")
                    .to_owned(),
            )
            .await
    }
}
