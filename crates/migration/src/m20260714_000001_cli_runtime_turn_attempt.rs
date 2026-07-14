use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table("turn_cli_runtime_attempt")
                    .if_not_exists()
                    .col(string("id").string_len(21).primary_key())
                    .col(string("turn_id").string_len(21))
                    .col(integer("attempt_index"))
                    .col(string("runtime_id").string_len(128))
                    .col(string("runtime_kind").string_len(64))
                    .col(text("native_thread_id"))
                    .col(text("native_turn_id").null())
                    .col(string("recovery_job_id").string_len(21).null())
                    .col(string("recovery_attempt_id").string_len(21).null())
                    .col(integer("execution_window_index").null())
                    .col(string("status").string_len(32))
                    .col(text("failure_reason").null())
                    .col(timestamp_with_time_zone("started_at").null())
                    .col(timestamp_with_time_zone("completed_at").null())
                    .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                    .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                    .to_owned(),
            )
            .await?;

        create_index(
            manager,
            "uidx_turn_cli_runtime_attempt_turn_index",
            &["turn_id", "attempt_index"],
            true,
        )
        .await?;
        create_index(
            manager,
            "uidx_turn_cli_runtime_attempt_recovery_attempt",
            &["recovery_attempt_id"],
            true,
        )
        .await?;
        create_index(
            manager,
            "uidx_turn_cli_runtime_attempt_runtime_native_turn",
            &["runtime_id", "native_turn_id"],
            true,
        )
        .await?;
        create_index(
            manager,
            "idx_turn_cli_runtime_attempt_turn_status",
            &["turn_id", "status"],
            false,
        )
        .await?;
        create_index(
            manager,
            "idx_turn_cli_runtime_attempt_recovery_job",
            &["recovery_job_id"],
            false,
        )
        .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .if_exists()
                    .table("turn_cli_runtime_attempt")
                    .to_owned(),
            )
            .await
    }
}

async fn create_index(
    manager: &SchemaManager<'_>,
    name: &str,
    columns: &[&str],
    unique: bool,
) -> Result<(), DbErr> {
    let mut statement = Index::create();
    statement
        .if_not_exists()
        .name(name)
        .table("turn_cli_runtime_attempt");
    for column in columns {
        statement.col(Alias::new(*column));
    }
    if unique {
        statement.unique();
    }
    manager.create_index(statement.to_owned()).await
}
