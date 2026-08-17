use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table("turn_execution")
                    .if_not_exists()
                    .col(string("turn_id").string_len(21).primary_key())
                    .col(string("thread_id").string_len(21))
                    .col(string("workspace_id").string_len(21))
                    .col(string("executor_kind").string_len(32))
                    .col(text("executor_key").null())
                    .col(string("status").string_len(32))
                    .col(string("owner_id").string_len(128))
                    .col(big_integer("owner_generation").default(1))
                    .col(timestamp_with_time_zone("lease_until"))
                    .col(timestamp_with_time_zone("heartbeat_at"))
                    .col(timestamp_with_time_zone("started_at").null())
                    .col(timestamp_with_time_zone("completed_at").null())
                    .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                    .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_turn_execution_turn")
                            .from("turn_execution", "turn_id")
                            .to("turn", "id")
                            .on_update(ForeignKeyAction::NoAction)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .check((
                        "ck_turn_execution_executor_kind",
                        Expr::cust(
                            "executor_kind IN ('native_agent', 'api_provider', 'cli_runtime', 'acp_runtime')",
                        ),
                    ))
                    .check((
                        "ck_turn_execution_status",
                        Expr::cust(
                            "status IN ('queued', 'starting', 'running', 'recovering', 'completed', 'failed', 'interrupted', 'blocked')",
                        ),
                    ))
                    .check((
                        "ck_turn_execution_owner_generation",
                        Expr::cust("owner_generation > 0"),
                    ))
                    .check((
                        "ck_turn_execution_owner",
                        Expr::cust("length(trim(owner_id)) > 0"),
                    ))
                    .check((
                        "ck_turn_execution_lease_order",
                        Expr::cust("lease_until >= heartbeat_at"),
                    ))
                    .check((
                        "ck_turn_execution_started_state",
                        Expr::cust("status != 'running' OR started_at IS NOT NULL"),
                    ))
                    .check((
                        "ck_turn_execution_terminal_state",
                        Expr::cust(
                            "(status IN ('queued', 'starting', 'running', 'recovering') AND completed_at IS NULL) OR \
                             (status IN ('completed', 'failed', 'interrupted', 'blocked') AND completed_at IS NOT NULL)",
                        ),
                    ))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_turn_execution_status_lease")
                    .table("turn_execution")
                    .col("status")
                    .col("lease_until")
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_turn_execution_owner_status")
                    .table("turn_execution")
                    .col("owner_id")
                    .col("status")
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_turn_execution_thread")
                    .table("turn_execution")
                    .col("thread_id")
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for name in [
            "idx_turn_execution_thread",
            "idx_turn_execution_owner_status",
            "idx_turn_execution_status_lease",
        ] {
            manager
                .drop_index(
                    Index::drop()
                        .if_exists()
                        .name(name)
                        .table("turn_execution")
                        .to_owned(),
                )
                .await?;
        }
        manager
            .drop_table(Table::drop().if_exists().table("turn_execution").to_owned())
            .await
    }
}
