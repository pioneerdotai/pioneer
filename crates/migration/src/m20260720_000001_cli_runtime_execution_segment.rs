use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(Alias::new("turn_cli_runtime_execution_segment"))
                    .if_not_exists()
                    .col(string("id").string_len(21).primary_key())
                    .col(string("attempt_id").string_len(21).not_null())
                    .col(string("turn_id").string_len(21).not_null())
                    .col(integer("segment_index").not_null())
                    .col(string("runtime_id").string_len(128).not_null())
                    .col(text("native_thread_id").not_null())
                    .col(text("native_turn_id").not_null())
                    .col(string("status").string_len(32).not_null())
                    .col(text("failure_reason").null())
                    .col(timestamp_with_time_zone("started_at").not_null())
                    .col(timestamp_with_time_zone("completed_at").null())
                    .col(
                        timestamp_with_time_zone("created_at")
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .col(
                        timestamp_with_time_zone("updated_at")
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_turn_cli_runtime_execution_segment_attempt")
                            .from(
                                Alias::new("turn_cli_runtime_execution_segment"),
                                Alias::new("attempt_id"),
                            )
                            .to(Alias::new("turn_cli_runtime_attempt"), Alias::new("id"))
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;

        for (name, columns, unique) in [
            (
                "uidx_turn_cli_runtime_execution_segment_attempt_index",
                &["attempt_id", "segment_index"][..],
                true,
            ),
            (
                "uidx_turn_cli_runtime_execution_segment_runtime_native_turn",
                &["runtime_id", "native_turn_id"][..],
                true,
            ),
            (
                "idx_turn_cli_runtime_execution_segment_turn_status",
                &["turn_id", "status"][..],
                false,
            ),
            (
                "idx_turn_cli_runtime_execution_segment_attempt_status",
                &["attempt_id", "status"][..],
                false,
            ),
        ] {
            let mut statement = Index::create();
            statement
                .if_not_exists()
                .name(name)
                .table(Alias::new("turn_cli_runtime_execution_segment"));
            for column in columns {
                statement.col(Alias::new(*column));
            }
            if unique {
                statement.unique();
            }
            manager.create_index(statement.to_owned()).await?;
        }

        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("uidx_turn_cli_runtime_execution_segment_turn_running")
                    .table(Alias::new("turn_cli_runtime_execution_segment"))
                    .col(Alias::new("turn_id"))
                    .unique()
                    .cond_where(Expr::col(Alias::new("status")).eq("running"))
                    .to_owned(),
            )
            .await?;

        for (name, column) in [
            (
                "native_goal_status",
                string("native_goal_status")
                    .string_len(32)
                    .null()
                    .to_owned(),
            ),
            (
                "native_goal_turn_id",
                text("native_goal_turn_id").null().to_owned(),
            ),
            (
                "native_goal_observed_at",
                timestamp_with_time_zone("native_goal_observed_at")
                    .null()
                    .to_owned(),
            ),
        ] {
            if !manager.has_column("turn_cli_runtime_binding", name).await? {
                manager
                    .alter_table(
                        Table::alter()
                            .table(Alias::new("turn_cli_runtime_binding"))
                            .add_column(column)
                            .to_owned(),
                    )
                    .await?;
            }
        }

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for column in [
            "native_goal_observed_at",
            "native_goal_turn_id",
            "native_goal_status",
        ] {
            if manager
                .has_column("turn_cli_runtime_binding", column)
                .await?
            {
                manager
                    .alter_table(
                        Table::alter()
                            .table(Alias::new("turn_cli_runtime_binding"))
                            .drop_column(Alias::new(column))
                            .to_owned(),
                    )
                    .await?;
            }
        }

        manager
            .drop_table(
                Table::drop()
                    .table(Alias::new("turn_cli_runtime_execution_segment"))
                    .if_exists()
                    .to_owned(),
            )
            .await
    }
}
