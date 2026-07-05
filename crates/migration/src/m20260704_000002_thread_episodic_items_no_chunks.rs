use sea_orm_migration::{
    prelude::*,
    schema::*,
    sea_orm::{ConnectionTrait, Statement},
};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        rename_column_if_exists(
            manager,
            "thread_episodic_capsules",
            "active_chunk_count",
            "active_frame_count",
        )
        .await?;

        rename_column_if_exists(
            manager,
            "thread_episodic_thread_directory",
            "indexed_chunk_count",
            "indexed_item_count",
        )
        .await?;

        drop_thread_episodic_derived_tables(manager).await?;

        create_thread_episodic_items_table(manager).await?;
        create_thread_episodic_item_index_jobs_table(manager).await?;
        create_thread_episodic_item_exclusions_table(manager).await?;
        create_thread_episodic_item_indexes(manager).await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        rename_column_if_exists(
            manager,
            "thread_episodic_capsules",
            "active_frame_count",
            "active_chunk_count",
        )
        .await?;

        rename_column_if_exists(
            manager,
            "thread_episodic_thread_directory",
            "indexed_item_count",
            "indexed_chunk_count",
        )
        .await?;

        drop_thread_episodic_derived_tables(manager).await?;

        create_thread_episodic_chunks_table(manager).await?;
        create_thread_episodic_chunk_index_jobs_table(manager).await?;
        create_thread_episodic_chunk_exclusions_table(manager).await?;
        create_thread_episodic_chunk_indexes(manager).await
    }
}

async fn rename_column_if_exists(
    manager: &SchemaManager<'_>,
    table: &str,
    from: &str,
    to: &str,
) -> Result<(), DbErr> {
    if column_exists(manager, table, from).await? && !column_exists(manager, table, to).await? {
        let sql = format!("ALTER TABLE {table} RENAME COLUMN {from} TO {to}");
        manager
            .get_connection()
            .execute_unprepared(sql.as_str())
            .await?;
    }
    Ok(())
}

async fn column_exists(
    manager: &SchemaManager<'_>,
    table: &str,
    column: &str,
) -> Result<bool, DbErr> {
    let sql = format!("SELECT 1 FROM pragma_table_info('{table}') WHERE name = '{column}' LIMIT 1");
    let statement = Statement::from_string(manager.get_database_backend(), sql);
    let row = manager.get_connection().query_one_raw(statement).await?;
    Ok(row.is_some())
}

async fn drop_thread_episodic_derived_tables(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    for index in [
        "uidx_thread_episodic_exclusions_chunk",
        "uidx_thread_episodic_exclusions_item",
        "uidx_thread_episodic_index_jobs_chunk_capsule",
        "uidx_thread_episodic_index_jobs_item_capsule",
        "idx_thread_episodic_index_jobs_chunk",
        "idx_thread_episodic_index_jobs_item",
        "idx_thread_episodic_index_jobs_workspace_thread_status",
        "idx_thread_episodic_index_jobs_due",
        "uidx_thread_episodic_chunks_source_hash",
        "uidx_thread_episodic_items_source_hash",
        "idx_thread_episodic_chunks_status_updated",
        "idx_thread_episodic_items_status_updated",
        "idx_thread_episodic_chunks_text_hash",
        "idx_thread_episodic_items_text_hash",
        "idx_thread_episodic_chunks_thread_turn",
        "idx_thread_episodic_items_thread_turn",
        "idx_thread_episodic_chunks_workspace_thread_status",
        "idx_thread_episodic_items_workspace_thread_status",
    ] {
        manager
            .drop_index(Index::drop().if_exists().name(index).to_owned())
            .await?;
    }

    for table in [
        "thread_episodic_exclusions",
        "thread_episodic_index_jobs",
        "thread_episodic_chunks",
        "thread_episodic_items",
    ] {
        manager
            .drop_table(Table::drop().if_exists().table(table).to_owned())
            .await?;
    }

    Ok(())
}

async fn create_thread_episodic_items_table(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table("thread_episodic_items")
                .if_not_exists()
                .col(string("id").string_len(21).primary_key())
                .col(string("workspace_id").string_len(21))
                .col(string("thread_id").string_len(21))
                .col(string("turn_id").string_len(21))
                .col(string("item_id").string_len(128))
                .col(string("source_actor_role").string_len(32))
                .col(string("source_runtime_kind").string_len(64))
                .col(text("source_context_json"))
                .col(string("visibility").string_len(32))
                .col(string("status").string_len(32))
                .col(string("text_hash").string_len(128))
                .col(string("source_text_hash").string_len(128))
                .col(string("language_hint").string_len(32).null())
                .col(integer("token_estimate"))
                .col(string("capsule_id").string_len(21).null())
                .col(text("capsule_ref").null())
                .col(integer("segment_index").null())
                .col(big_integer("frame_id").null())
                .col(text("frame_uri").null())
                .col(timestamp_with_time_zone("indexed_at").null())
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("deleted_at").null())
                .to_owned(),
        )
        .await
}

async fn create_thread_episodic_item_index_jobs_table(
    manager: &SchemaManager<'_>,
) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table("thread_episodic_index_jobs")
                .if_not_exists()
                .col(string("id").string_len(21).primary_key())
                .col(string("workspace_id").string_len(21))
                .col(string("thread_id").string_len(21))
                .col(string("index_item_id").string_len(21))
                .col(string("capsule_id").string_len(21).null())
                .col(text("capsule_ref").null())
                .col(integer("segment_index").null())
                .col(text("frame_uri").null())
                .col(string("status").string_len(32).default("queued"))
                .col(
                    string("graph_enrichment_state")
                        .string_len(32)
                        .default("not_supported"),
                )
                .col(integer("attempt_count").default(0))
                .col(integer("capacity_error_count").default(0))
                .col(integer("last_attempt_latency_ms").null())
                .col(timestamp_with_time_zone("next_run_at").default(Expr::current_timestamp()))
                .col(text("last_error").null())
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("completed_at").null())
                .to_owned(),
        )
        .await
}

async fn create_thread_episodic_item_exclusions_table(
    manager: &SchemaManager<'_>,
) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table("thread_episodic_exclusions")
                .if_not_exists()
                .col(string("id").string_len(21).primary_key())
                .col(string("workspace_id").string_len(21))
                .col(string("thread_id").string_len(21))
                .col(string("index_item_id").string_len(21))
                .col(string("reason").string_len(32))
                .col(string("created_by").string_len(128))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .to_owned(),
        )
        .await
}

async fn create_thread_episodic_chunks_table(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table("thread_episodic_chunks")
                .if_not_exists()
                .col(string("id").string_len(21).primary_key())
                .col(string("workspace_id").string_len(21))
                .col(string("thread_id").string_len(21))
                .col(string("turn_id").string_len(21))
                .col(string("item_id").string_len(128))
                .col(integer("chunk_index"))
                .col(integer("chunk_count"))
                .col(string("source_actor_role").string_len(32))
                .col(string("source_runtime_kind").string_len(64))
                .col(text("source_context_json"))
                .col(string("visibility").string_len(32))
                .col(string("status").string_len(32))
                .col(string("text_hash").string_len(128))
                .col(string("source_text_hash").string_len(128))
                .col(big_integer("char_start"))
                .col(big_integer("char_end"))
                .col(big_integer("byte_start").null())
                .col(big_integer("byte_end").null())
                .col(string("language_hint").string_len(32).null())
                .col(integer("token_estimate"))
                .col(string("capsule_id").string_len(21).null())
                .col(text("capsule_ref").null())
                .col(integer("segment_index").null())
                .col(big_integer("frame_id").null())
                .col(text("frame_uri").null())
                .col(timestamp_with_time_zone("indexed_at").null())
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("deleted_at").null())
                .to_owned(),
        )
        .await
}

async fn create_thread_episodic_chunk_index_jobs_table(
    manager: &SchemaManager<'_>,
) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table("thread_episodic_index_jobs")
                .if_not_exists()
                .col(string("id").string_len(21).primary_key())
                .col(string("workspace_id").string_len(21))
                .col(string("thread_id").string_len(21))
                .col(string("chunk_id").string_len(21))
                .col(string("capsule_id").string_len(21).null())
                .col(text("capsule_ref").null())
                .col(integer("segment_index").null())
                .col(text("frame_uri").null())
                .col(string("status").string_len(32).default("queued"))
                .col(
                    string("graph_enrichment_state")
                        .string_len(32)
                        .default("not_supported"),
                )
                .col(integer("attempt_count").default(0))
                .col(integer("capacity_error_count").default(0))
                .col(integer("last_attempt_latency_ms").null())
                .col(timestamp_with_time_zone("next_run_at").default(Expr::current_timestamp()))
                .col(text("last_error").null())
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("completed_at").null())
                .to_owned(),
        )
        .await
}

async fn create_thread_episodic_chunk_exclusions_table(
    manager: &SchemaManager<'_>,
) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table("thread_episodic_exclusions")
                .if_not_exists()
                .col(string("id").string_len(21).primary_key())
                .col(string("workspace_id").string_len(21))
                .col(string("thread_id").string_len(21))
                .col(string("chunk_id").string_len(21))
                .col(string("reason").string_len(32))
                .col(string("created_by").string_len(128))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .to_owned(),
        )
        .await
}

async fn create_thread_episodic_item_indexes(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    create_index(
        manager,
        "idx_thread_episodic_items_workspace_thread_status",
        "thread_episodic_items",
        &["workspace_id", "thread_id", "status"],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_thread_episodic_items_thread_turn",
        "thread_episodic_items",
        &["workspace_id", "thread_id", "turn_id"],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_thread_episodic_items_text_hash",
        "thread_episodic_items",
        &["workspace_id", "thread_id", "text_hash"],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_thread_episodic_items_status_updated",
        "thread_episodic_items",
        &["status", "updated_at"],
        false,
    )
    .await?;
    create_index(
        manager,
        "uidx_thread_episodic_items_source_hash",
        "thread_episodic_items",
        &[
            "workspace_id",
            "thread_id",
            "turn_id",
            "item_id",
            "text_hash",
        ],
        true,
    )
    .await?;
    create_index(
        manager,
        "idx_thread_episodic_index_jobs_due",
        "thread_episodic_index_jobs",
        &["status", "next_run_at"],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_thread_episodic_index_jobs_workspace_thread_status",
        "thread_episodic_index_jobs",
        &["workspace_id", "thread_id", "status"],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_thread_episodic_index_jobs_item",
        "thread_episodic_index_jobs",
        &["index_item_id"],
        false,
    )
    .await?;
    create_index(
        manager,
        "uidx_thread_episodic_index_jobs_item_capsule",
        "thread_episodic_index_jobs",
        &["index_item_id", "capsule_id"],
        true,
    )
    .await?;
    create_index(
        manager,
        "uidx_thread_episodic_exclusions_item",
        "thread_episodic_exclusions",
        &["workspace_id", "thread_id", "index_item_id"],
        true,
    )
    .await
}

async fn create_thread_episodic_chunk_indexes(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    create_index(
        manager,
        "idx_thread_episodic_chunks_workspace_thread_status",
        "thread_episodic_chunks",
        &["workspace_id", "thread_id", "status"],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_thread_episodic_chunks_thread_turn",
        "thread_episodic_chunks",
        &["workspace_id", "thread_id", "turn_id"],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_thread_episodic_chunks_text_hash",
        "thread_episodic_chunks",
        &["workspace_id", "thread_id", "text_hash"],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_thread_episodic_chunks_status_updated",
        "thread_episodic_chunks",
        &["status", "updated_at"],
        false,
    )
    .await?;
    create_index(
        manager,
        "uidx_thread_episodic_chunks_source_hash",
        "thread_episodic_chunks",
        &[
            "workspace_id",
            "thread_id",
            "turn_id",
            "item_id",
            "chunk_index",
            "text_hash",
        ],
        true,
    )
    .await?;
    create_index(
        manager,
        "idx_thread_episodic_index_jobs_due",
        "thread_episodic_index_jobs",
        &["status", "next_run_at"],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_thread_episodic_index_jobs_workspace_thread_status",
        "thread_episodic_index_jobs",
        &["workspace_id", "thread_id", "status"],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_thread_episodic_index_jobs_chunk",
        "thread_episodic_index_jobs",
        &["chunk_id"],
        false,
    )
    .await?;
    create_index(
        manager,
        "uidx_thread_episodic_index_jobs_chunk_capsule",
        "thread_episodic_index_jobs",
        &["chunk_id", "capsule_id"],
        true,
    )
    .await?;
    create_index(
        manager,
        "uidx_thread_episodic_exclusions_chunk",
        "thread_episodic_exclusions",
        &["workspace_id", "thread_id", "chunk_id"],
        true,
    )
    .await
}

async fn create_index(
    manager: &SchemaManager<'_>,
    name: &str,
    table: &str,
    columns: &[&str],
    unique: bool,
) -> Result<(), DbErr> {
    let mut index = Index::create()
        .if_not_exists()
        .name(name)
        .table(Alias::new(table))
        .to_owned();
    for column in columns {
        index.col(Alias::new(*column));
    }
    if unique {
        index.unique();
    }
    manager.create_index(index).await
}
