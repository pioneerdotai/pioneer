use sea_orm_migration::{prelude::*, schema::*, sea_orm::ConnectionTrait};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        create_thread_episodic_capsules_table(manager).await?;
        create_thread_episodic_chunks_table(manager).await?;
        create_thread_episodic_index_jobs_table(manager).await?;
        create_thread_episodic_exclusions_table(manager).await?;
        create_thread_episodic_recall_events_table(manager).await?;
        create_thread_episodic_thread_directory_table(manager).await?;
        create_thread_episodic_indexes(manager).await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .if_exists()
                    .table("thread_episodic_thread_directory")
                    .to_owned(),
            )
            .await?;

        manager
            .drop_table(
                Table::drop()
                    .if_exists()
                    .table("thread_episodic_recall_events")
                    .to_owned(),
            )
            .await?;

        manager
            .drop_table(
                Table::drop()
                    .if_exists()
                    .table("thread_episodic_exclusions")
                    .to_owned(),
            )
            .await?;

        manager
            .drop_table(
                Table::drop()
                    .if_exists()
                    .table("thread_episodic_index_jobs")
                    .to_owned(),
            )
            .await?;

        manager
            .drop_table(
                Table::drop()
                    .if_exists()
                    .table("thread_episodic_chunks")
                    .to_owned(),
            )
            .await?;

        manager
            .drop_table(
                Table::drop()
                    .if_exists()
                    .table("thread_episodic_capsules")
                    .to_owned(),
            )
            .await
    }
}

async fn create_thread_episodic_capsules_table(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table("thread_episodic_capsules")
                .if_not_exists()
                .col(string("id").string_len(21).primary_key())
                .col(string("workspace_id").string_len(21))
                .col(string("workspace_key_hash").string_len(128))
                .col(string("thread_id").string_len(21))
                .col(string("thread_key_hash").string_len(128))
                .col(integer("segment_index"))
                .col(string("write_state").string_len(32))
                .col(text("capsule_ref"))
                .col(text("storage_uri"))
                .col(string("backend").string_len(32).default("memvid"))
                .col(string("format").string_len(32).default("mv2"))
                .col(boolean("encrypted").default(false))
                .col(string("status").string_len(32).default("active"))
                .col(string("repair_status").string_len(32).default("ok"))
                .col(big_integer("active_chunk_count").default(0))
                .col(big_integer("capacity_bytes").null())
                .col(big_integer("size_bytes").null())
                .col(double("utilization_percent").null())
                .col(timestamp_with_time_zone("last_capacity_check_at").null())
                .col(timestamp_with_time_zone("near_capacity_at").null())
                .col(timestamp_with_time_zone("capacity_exceeded_at").null())
                .col(timestamp_with_time_zone("last_vacuumed_at").null())
                .col(timestamp_with_time_zone("last_compacted_at").null())
                .col(string("content_hash").string_len(128).null())
                .col(text("metadata_json").null())
                .col(text("last_error").null())
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .to_owned(),
        )
        .await
}

async fn create_thread_episodic_exclusions_table(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
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

async fn create_thread_episodic_recall_events_table(
    manager: &SchemaManager<'_>,
) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table("thread_episodic_recall_events")
                .if_not_exists()
                .col(string("id").string_len(21).primary_key())
                .col(string("workspace_id").string_len(21))
                .col(string("thread_id").string_len(21))
                .col(string("turn_id").string_len(21))
                .col(string("query_hash").string_len(128).null())
                .col(text("search_profile_json").null())
                .col(string("search_mode").string_len(32).null())
                .col(string("adaptive_strategy").string_len(32).null())
                .col(text("cutoff_json").null())
                .col(integer("candidate_count").default(0))
                .col(integer("returned_count").default(0))
                .col(integer("latency_ms").default(0))
                .col(boolean("fallback_used").default(false))
                .col(text("error").null())
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .to_owned(),
        )
        .await
}

async fn create_thread_episodic_thread_directory_table(
    manager: &SchemaManager<'_>,
) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table("thread_episodic_thread_directory")
                .if_not_exists()
                .col(string("id").string_len(21).primary_key())
                .col(string("workspace_id").string_len(21))
                .col(string("thread_id").string_len(21))
                .col(string("title").string_len(512).null())
                .col(string("summary_hash").string_len(128).null())
                .col(text("summary_ref").null())
                .col(timestamp_with_time_zone("thread_created_at").null())
                .col(timestamp_with_time_zone("thread_updated_at").null())
                .col(timestamp_with_time_zone("last_indexed_at").null())
                .col(integer("indexed_chunk_count").default(0))
                .col(text("task_affinity_json").null())
                .col(text("project_affinity_json").null())
                .col(string("visibility").string_len(32).default("visible"))
                .col(string("status").string_len(32).default("active"))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .to_owned(),
        )
        .await
}

async fn create_thread_episodic_index_jobs_table(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
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

async fn create_thread_episodic_indexes(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    create_index(
        manager,
        "uidx_thread_episodic_capsules_ref",
        "thread_episodic_capsules",
        &["capsule_ref"],
        true,
    )
    .await?;
    create_index(
        manager,
        "uidx_thread_episodic_capsules_segment",
        "thread_episodic_capsules",
        &["workspace_id", "thread_id", "segment_index"],
        true,
    )
    .await?;

    // SeaQuery's portable index builder does not model this SQLite partial unique
    // predicate cleanly. Keep raw SQL narrowly scoped to the predicate-only case.
    manager
        .get_connection()
        .execute_unprepared(
            "CREATE UNIQUE INDEX IF NOT EXISTS uidx_thread_episodic_capsules_active_write \
             ON thread_episodic_capsules (workspace_id, thread_id) \
             WHERE write_state = 'active_write' AND status != 'deleted'",
        )
        .await?;

    create_index(
        manager,
        "idx_thread_episodic_capsules_write_state",
        "thread_episodic_capsules",
        &["workspace_id", "thread_id", "write_state"],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_thread_episodic_capsules_workspace_status",
        "thread_episodic_capsules",
        &["workspace_id", "status"],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_thread_episodic_capsules_repair_status",
        "thread_episodic_capsules",
        &["repair_status"],
        false,
    )
    .await?;
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
    .await?;
    create_index(
        manager,
        "idx_thread_episodic_recall_events_thread_created",
        "thread_episodic_recall_events",
        &["workspace_id", "thread_id", "created_at"],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_thread_episodic_recall_events_turn",
        "thread_episodic_recall_events",
        &["turn_id"],
        false,
    )
    .await?;
    create_index(
        manager,
        "uidx_thread_episodic_thread_directory_thread",
        "thread_episodic_thread_directory",
        &["workspace_id", "thread_id"],
        true,
    )
    .await?;
    create_index(
        manager,
        "idx_thread_episodic_thread_directory_workspace_status",
        "thread_episodic_thread_directory",
        &["workspace_id", "status", "visibility", "last_indexed_at"],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_thread_episodic_thread_directory_workspace_updated",
        "thread_episodic_thread_directory",
        &["workspace_id", "thread_updated_at"],
        false,
    )
    .await?;

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
