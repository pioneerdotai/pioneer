use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        create_thread_cli_runtime_binding_table(manager).await?;
        create_turn_cli_runtime_binding_table(manager).await?;
        create_cli_runtime_pending_request_table(manager).await?;
        create_cli_runtime_native_event_table(manager).await?;
        create_cli_runtime_binding_indexes(manager).await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        drop_cli_runtime_binding_indexes(manager).await?;

        for table_name in [
            "cli_runtime_native_event",
            "cli_runtime_pending_request",
            "turn_cli_runtime_binding",
            "thread_cli_runtime_binding",
        ] {
            manager
                .drop_table(
                    Table::drop()
                        .if_exists()
                        .table(Alias::new(table_name))
                        .to_owned(),
                )
                .await?;
        }

        Ok(())
    }
}

async fn create_thread_cli_runtime_binding_table(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table("thread_cli_runtime_binding")
                .if_not_exists()
                .col(string("thread_id").string_len(21).primary_key())
                .col(string("workspace_id").string_len(21))
                .col(string("runtime_id").string_len(128))
                .col(string("runtime_kind").string_len(64))
                .col(text("native_thread_id"))
                .col(text("native_session_id").null())
                .col(text("native_root_thread_id").null())
                .col(text("native_cwd").null())
                .col(text("native_model").null())
                .col(text("resume_cursor_json").default("{}"))
                .col(string("status").string_len(32))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .to_owned(),
        )
        .await
}

async fn create_turn_cli_runtime_binding_table(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table("turn_cli_runtime_binding")
                .if_not_exists()
                .col(string("turn_id").string_len(21).primary_key())
                .col(string("thread_id").string_len(21))
                .col(string("workspace_id").string_len(21))
                .col(string("runtime_id").string_len(128))
                .col(string("runtime_kind").string_len(64))
                .col(text("native_thread_id"))
                .col(text("native_turn_id").null())
                .col(text("request_id").null())
                .col(string("status").string_len(32))
                .col(text("model").null())
                .col(text("cwd").null())
                .col(text("sandbox_json").null())
                .col(string("approval_policy").string_len(64).null())
                .col(text("input_mapping_json").default("{}"))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .to_owned(),
        )
        .await
}

async fn create_cli_runtime_pending_request_table(
    manager: &SchemaManager<'_>,
) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table("cli_runtime_pending_request")
                .if_not_exists()
                .col(text("request_id").primary_key())
                .col(string("runtime_id").string_len(128))
                .col(string("runtime_kind").string_len(64))
                .col(string("workspace_id").string_len(21))
                .col(string("thread_id").string_len(21))
                .col(string("turn_id").string_len(21).null())
                .col(text("native_thread_id").null())
                .col(text("native_turn_id").null())
                .col(text("native_item_id").null())
                .col(string("request_kind").string_len(64))
                .col(text("payload_json"))
                .col(string("status").string_len(32))
                .col(text("response_json").null())
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("resolved_at").null())
                .to_owned(),
        )
        .await
}

async fn create_cli_runtime_native_event_table(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table("cli_runtime_native_event")
                .if_not_exists()
                .col(string("id").string_len(96).primary_key())
                .col(string("runtime_id").string_len(128))
                .col(string("runtime_kind").string_len(64))
                .col(string("workspace_id").string_len(21).null())
                .col(string("thread_id").string_len(21).null())
                .col(string("turn_id").string_len(21).null())
                .col(text("native_thread_id").null())
                .col(text("native_turn_id").null())
                .col(text("native_method"))
                .col(text("payload_redacted_json"))
                .col(big_integer("sequence"))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .to_owned(),
        )
        .await
}

async fn create_cli_runtime_binding_indexes(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    create_index(
        manager,
        "idx_thread_cli_runtime_binding_workspace_runtime",
        "thread_cli_runtime_binding",
        &["workspace_id", "runtime_id", "status"],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_thread_cli_runtime_binding_runtime_native_thread",
        "thread_cli_runtime_binding",
        &["runtime_id", "native_thread_id"],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_thread_cli_runtime_binding_workspace_status",
        "thread_cli_runtime_binding",
        &["workspace_id", "status"],
        false,
    )
    .await?;

    create_index(
        manager,
        "idx_turn_cli_runtime_binding_thread",
        "turn_cli_runtime_binding",
        &["thread_id"],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_turn_cli_runtime_binding_runtime_native_turn",
        "turn_cli_runtime_binding",
        &["runtime_id", "native_turn_id"],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_turn_cli_runtime_binding_native_thread",
        "turn_cli_runtime_binding",
        &["runtime_id", "native_thread_id"],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_turn_cli_runtime_binding_request",
        "turn_cli_runtime_binding",
        &["request_id"],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_turn_cli_runtime_binding_workspace_status",
        "turn_cli_runtime_binding",
        &["workspace_id", "status"],
        false,
    )
    .await?;

    create_index(
        manager,
        "idx_cli_runtime_pending_request_thread_turn",
        "cli_runtime_pending_request",
        &["workspace_id", "thread_id", "turn_id"],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_cli_runtime_pending_request_runtime_status",
        "cli_runtime_pending_request",
        &["runtime_id", "status", "updated_at"],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_cli_runtime_pending_request_native_turn",
        "cli_runtime_pending_request",
        &["runtime_id", "native_thread_id", "native_turn_id"],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_cli_runtime_pending_request_status_created",
        "cli_runtime_pending_request",
        &["status", "created_at"],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_cli_runtime_pending_request_kind",
        "cli_runtime_pending_request",
        &["request_kind", "status"],
        false,
    )
    .await?;

    create_index(
        manager,
        "idx_cli_runtime_native_event_thread_sequence",
        "cli_runtime_native_event",
        &["runtime_id", "thread_id", "turn_id", "sequence"],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_cli_runtime_native_event_native_thread",
        "cli_runtime_native_event",
        &[
            "runtime_id",
            "native_thread_id",
            "native_turn_id",
            "sequence",
        ],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_cli_runtime_native_event_method",
        "cli_runtime_native_event",
        &["runtime_id", "native_method", "created_at"],
        false,
    )
    .await
}

async fn drop_cli_runtime_binding_indexes(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    for (table_name, index_name) in [
        (
            "cli_runtime_native_event",
            "idx_cli_runtime_native_event_method",
        ),
        (
            "cli_runtime_native_event",
            "idx_cli_runtime_native_event_native_thread",
        ),
        (
            "cli_runtime_native_event",
            "idx_cli_runtime_native_event_thread_sequence",
        ),
        (
            "cli_runtime_pending_request",
            "idx_cli_runtime_pending_request_kind",
        ),
        (
            "cli_runtime_pending_request",
            "idx_cli_runtime_pending_request_status_created",
        ),
        (
            "cli_runtime_pending_request",
            "idx_cli_runtime_pending_request_native_turn",
        ),
        (
            "cli_runtime_pending_request",
            "idx_cli_runtime_pending_request_runtime_status",
        ),
        (
            "cli_runtime_pending_request",
            "idx_cli_runtime_pending_request_thread_turn",
        ),
        (
            "turn_cli_runtime_binding",
            "idx_turn_cli_runtime_binding_workspace_status",
        ),
        (
            "turn_cli_runtime_binding",
            "idx_turn_cli_runtime_binding_request",
        ),
        (
            "turn_cli_runtime_binding",
            "idx_turn_cli_runtime_binding_native_thread",
        ),
        (
            "turn_cli_runtime_binding",
            "idx_turn_cli_runtime_binding_runtime_native_turn",
        ),
        (
            "turn_cli_runtime_binding",
            "idx_turn_cli_runtime_binding_thread",
        ),
        (
            "thread_cli_runtime_binding",
            "idx_thread_cli_runtime_binding_workspace_status",
        ),
        (
            "thread_cli_runtime_binding",
            "idx_thread_cli_runtime_binding_runtime_native_thread",
        ),
        (
            "thread_cli_runtime_binding",
            "idx_thread_cli_runtime_binding_workspace_runtime",
        ),
    ] {
        manager
            .drop_index(
                Index::drop()
                    .if_exists()
                    .name(index_name)
                    .table(Alias::new(table_name))
                    .to_owned(),
            )
            .await?;
    }

    Ok(())
}

async fn create_index(
    manager: &SchemaManager<'_>,
    name: &str,
    table: &str,
    columns: &[&str],
    unique: bool,
) -> Result<(), DbErr> {
    let mut index = Index::create();
    index.if_not_exists().name(name).table(Alias::new(table));
    for column in columns {
        index.col(Alias::new(*column));
    }
    if unique {
        index.unique();
    }
    manager.create_index(index.to_owned()).await
}
