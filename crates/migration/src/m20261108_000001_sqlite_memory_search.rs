use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(Index::drop().name("idx_agent_memory_capsule").to_owned())
            .await?;
        manager
            .drop_index(
                Index::drop()
                    .name("idx_agent_memory_repair_capsule")
                    .to_owned(),
            )
            .await?;
        manager
            .drop_table(
                Table::drop()
                    .table(Alias::new("agent_memory_capsule"))
                    .to_owned(),
            )
            .await?;
        for column in ["capsule_id", "capsule_ref", "frame_id", "frame_uri"] {
            manager
                .alter_table(
                    Table::alter()
                        .table(Alias::new("agent_memory"))
                        .drop_column(Alias::new(column))
                        .to_owned(),
                )
                .await?;
        }
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("agent_memory"))
                    .rename_column(Alias::new("frame_version"), Alias::new("payload_version"))
                    .to_owned(),
            )
            .await?;
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("agent_memory_repair_job"))
                    .drop_column(Alias::new("capsule_id"))
                    .to_owned(),
            )
            .await?;
        // Native DROP COLUMN rewrites the affected SQLite records. This is
        // schema removal only: no canonical data scan/backfill is done in Rust.
        manager
            .drop_index(
                Index::drop()
                    .name("uidx_thread_episodic_index_jobs_item_capsule")
                    .to_owned(),
            )
            .await?;
        manager
            .drop_table(
                Table::drop()
                    .table(Alias::new("thread_episodic_capsules"))
                    .to_owned(),
            )
            .await?;
        for (table, columns) in [
            (
                "thread_episodic_items",
                &[
                    "capsule_id",
                    "capsule_ref",
                    "segment_index",
                    "frame_id",
                    "frame_uri",
                ][..],
            ),
            (
                "thread_episodic_index_jobs",
                &["capsule_id", "capsule_ref", "segment_index", "frame_uri"][..],
            ),
        ] {
            for column in columns {
                manager
                    .alter_table(
                        Table::alter()
                            .table(Alias::new(table))
                            .drop_column(Alias::new(*column))
                            .to_owned(),
                    )
                    .await?;
            }
        }
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("thread_episodic_index_jobs"))
                    .drop_column(Alias::new("capacity_error_count"))
                    .to_owned(),
            )
            .await?;
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("thread_episodic_index_jobs"))
                    .drop_column(Alias::new("graph_enrichment_state"))
                    .to_owned(),
            )
            .await?;
        // Both projections start empty. Canonical history is repopulated by the
        // existing bounded refill owner, never by a migration-wide data scan.
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("thread_episodic_items"))
                    .add_column(ColumnDef::new(Alias::new("search_text")).text().null())
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table(Alias::new("agent_memory_payloads"))
                    .col(string("memory_id").primary_key())
                    .col(string("scope_kind"))
                    .col(string("scope_key_hash"))
                    .col(text("content"))
                    .col(big_integer("revision").default(1))
                    .to_owned(),
            )
            .await?;
        // A seed only: legacy control rows are retired by bounded startup work,
        // never by an unbounded UPDATE during schema installation.
        let seed = Query::insert()
            .into_table(Alias::new("thread_timeline_projection_meta"))
            .columns(
                [
                    "projection_key",
                    "projection_version",
                    "status",
                    "source_thread_count",
                    "source_turn_count",
                    "source_turn_item_count",
                    "source_turn_event_count",
                    "created_at",
                    "updated_at",
                ]
                .map(Alias::new),
            )
            .values_panic([
                Expr::value("memory_sqlite_payload_reset_v1").into(),
                Expr::value(1_i64).into(),
                Expr::value("pending").into(),
                Expr::value(0_i64).into(),
                Expr::value(0_i64).into(),
                Expr::value(0_i64).into(),
                Expr::value(0_i64).into(),
                Expr::current_timestamp().into(),
                Expr::current_timestamp().into(),
            ])
            .to_owned();
        manager.get_connection().execute(&seed).await?;
        install_fts(
            manager,
            "thread_episodic_items",
            "thread_episodic_fts",
            "search_text",
        )
        .await?;
        install_fts(
            manager,
            "agent_memory_payloads",
            "agent_memory_fts",
            "content",
        )
        .await?;
        // All new searchable text starts NULL, so its derived directory count
        // starts zero too. Replace only this derived column, without a data
        // UPDATE/backfill in schema installation (native DDL rewrites metadata).
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("thread_episodic_thread_directory"))
                    .drop_column(Alias::new("indexed_item_count"))
                    .to_owned(),
            )
            .await?;
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("thread_episodic_thread_directory"))
                    .add_column(
                        ColumnDef::new(Alias::new("indexed_item_count"))
                            .big_integer()
                            .not_null()
                            .default(0),
                    )
                    .to_owned(),
            )
            .await?;
        install_directory_count(manager).await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for name in [
            "episodic_directory_count_insert",
            "episodic_directory_count_delete",
            "episodic_directory_count_update",
            "episodic_directory_count_initialize",
        ] {
            manager
                .get_connection()
                .execute_unprepared(&format!("DROP TRIGGER {name}"))
                .await?;
        }
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("thread_episodic_index_jobs"))
                    .add_column(
                        ColumnDef::new(Alias::new("graph_enrichment_state"))
                            .string_len(32)
                            .not_null()
                            .default("not_supported"),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("thread_episodic_index_jobs"))
                    .add_column(
                        ColumnDef::new(Alias::new("capacity_error_count"))
                            .big_integer()
                            .not_null()
                            .default(0),
                    )
                    .to_owned(),
            )
            .await?;
        let seed = Query::delete()
            .from_table(Alias::new("thread_timeline_projection_meta"))
            .and_where(Expr::col(Alias::new("projection_key")).eq("memory_sqlite_payload_reset_v1"))
            .to_owned();
        manager.get_connection().execute(&seed).await?;
        restore_episodic_capsule_schema(manager).await?;
        for table in ["thread_episodic_items", "thread_episodic_index_jobs"] {
            manager
                .alter_table(
                    Table::alter()
                        .table(Alias::new(table))
                        .add_column(
                            ColumnDef::new(Alias::new("capsule_id"))
                                .string_len(21)
                                .null(),
                        )
                        .to_owned(),
                )
                .await?;
            for column in ["capsule_ref", "frame_uri"] {
                manager
                    .alter_table(
                        Table::alter()
                            .table(Alias::new(table))
                            .add_column(ColumnDef::new(Alias::new(column)).text().null())
                            .to_owned(),
                    )
                    .await?;
            }
            manager
                .alter_table(
                    Table::alter()
                        .table(Alias::new(table))
                        .add_column(ColumnDef::new(Alias::new("segment_index")).integer().null())
                        .to_owned(),
                )
                .await?;
        }
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("thread_episodic_items"))
                    .add_column(ColumnDef::new(Alias::new("frame_id")).big_integer().null())
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("uidx_thread_episodic_index_jobs_item_capsule")
                    .table(Alias::new("thread_episodic_index_jobs"))
                    .col(Alias::new("index_item_id"))
                    .col(Alias::new("capsule_id"))
                    .unique()
                    .to_owned(),
            )
            .await?;
        // Restore schema only; discarded derived payloads cannot be recovered.
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("agent_memory"))
                    .rename_column(Alias::new("payload_version"), Alias::new("frame_version"))
                    .to_owned(),
            )
            .await?;
        for column in ["capsule_id", "capsule_ref", "frame_uri"] {
            let mut definition = ColumnDef::new(Alias::new(column));
            if column == "capsule_id" {
                definition.string_len(21);
            } else {
                definition.text();
            }
            definition.null();
            manager
                .alter_table(
                    Table::alter()
                        .table(Alias::new("agent_memory"))
                        .add_column(&mut definition)
                        .to_owned(),
                )
                .await?;
        }
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("agent_memory"))
                    .add_column(ColumnDef::new(Alias::new("frame_id")).big_integer().null())
                    .to_owned(),
            )
            .await?;
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("agent_memory_repair_job"))
                    .add_column(
                        ColumnDef::new(Alias::new("capsule_id"))
                            .string_len(21)
                            .null(),
                    )
                    .to_owned(),
            )
            .await?;
        for (table, index) in [
            ("agent_memory", "idx_agent_memory_capsule"),
            ("agent_memory_repair_job", "idx_agent_memory_repair_capsule"),
        ] {
            manager
                .create_index(
                    Index::create()
                        .name(index)
                        .table(Alias::new(table))
                        .col(Alias::new("capsule_id"))
                        .to_owned(),
                )
                .await?;
        }
        restore_agent_capsule_schema(manager).await?;
        for (source, fts) in [
            ("thread_episodic_items", "thread_episodic_fts"),
            ("agent_memory_payloads", "agent_memory_fts"),
        ] {
            for suffix in ["insert", "update", "delete"] {
                manager
                    .get_connection()
                    .execute_unprepared(&format!("DROP TRIGGER {source}_fts_{suffix}"))
                    .await?;
            }
            manager
                .get_connection()
                .execute_unprepared(&format!("DROP TABLE {fts}"))
                .await?;
        }
        manager
            .drop_table(
                Table::drop()
                    .table(Alias::new("agent_memory_payloads"))
                    .to_owned(),
            )
            .await?;
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("thread_episodic_items"))
                    .drop_column(Alias::new("search_text"))
                    .to_owned(),
            )
            .await
    }
}

async fn install_fts(
    manager: &SchemaManager<'_>,
    source: &str,
    fts: &str,
    text: &str,
) -> Result<(), DbErr> {
    // SeaQuery has no FTS5 virtual-table or trigger DDL API. Names here are
    // migration constants, never application inputs. External content avoids
    // storing a second full payload in the FTS index.
    for sql in [
        format!(
            "CREATE VIRTUAL TABLE {fts} USING fts5({text}, content='{source}', content_rowid='rowid', tokenize='unicode61')"
        ),
        format!(
            "CREATE TRIGGER {source}_fts_insert AFTER INSERT ON {source} WHEN new.{text} IS NOT NULL BEGIN INSERT INTO {fts}(rowid,{text}) VALUES(new.rowid,new.{text}); END"
        ),
        format!(
            "CREATE TRIGGER {source}_fts_delete AFTER DELETE ON {source} WHEN old.{text} IS NOT NULL BEGIN INSERT INTO {fts}({fts},rowid,{text}) VALUES('delete',old.rowid,old.{text}); END"
        ),
        format!(
            "CREATE TRIGGER {source}_fts_update AFTER UPDATE OF {text} ON {source} WHEN old.{text} IS NOT new.{text} BEGIN INSERT INTO {fts}({fts},rowid,{text}) SELECT 'delete',old.rowid,old.{text} WHERE old.{text} IS NOT NULL; INSERT INTO {fts}(rowid,{text}) SELECT new.rowid,new.{text} WHERE new.{text} IS NOT NULL; END"
        ),
    ] {
        manager.get_connection().execute_unprepared(&sql).await?;
    }
    Ok(())
}

async fn restore_agent_capsule_schema(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table("agent_memory_capsule")
                .if_not_exists()
                .col(string("id").string_len(21).primary_key())
                .col(string("scope_kind").string_len(32))
                .col(text("scope_key"))
                .col(string("scope_key_hash").string_len(128))
                .col(string("workspace_id").string_len(21).null())
                .col(string("scope_slot").string_len(64).default("primary"))
                .col(text("capsule_ref"))
                .col(text("storage_uri"))
                .col(string("backend").string_len(32).default("memvid"))
                .col(string("format").string_len(32).default("mv2"))
                .col(boolean("encrypted").default(false))
                .col(string("status").string_len(32).default("active"))
                .col(string("repair_status").string_len(32).default("ok"))
                .col(string("content_hash").string_len(128).null())
                .col(big_integer("active_record_count").default(0))
                .col(timestamp_with_time_zone("last_compacted_at").null())
                .col(timestamp_with_time_zone("last_reindexed_at").null())
                .col(timestamp_with_time_zone("last_verified_at").null())
                .col(text("last_error").null())
                .col(text("metadata_json").null())
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .to_owned(),
        )
        .await?;

    manager
        .create_index(
            Index::create()
                .name("uidx_agent_memory_capsule_ref")
                .table("agent_memory_capsule")
                .col("capsule_ref")
                .unique()
                .to_owned(),
        )
        .await?;

    manager
        .create_index(
            Index::create()
                .name("uidx_agent_memory_capsule_scope_slot")
                .table("agent_memory_capsule")
                .col("scope_kind")
                .col("scope_key_hash")
                .col("scope_slot")
                .unique()
                .to_owned(),
        )
        .await?;

    manager
        .create_index(
            Index::create()
                .name("idx_agent_memory_capsule_scope")
                .table("agent_memory_capsule")
                .col("scope_kind")
                .col("scope_key_hash")
                .to_owned(),
        )
        .await?;

    manager
        .create_index(
            Index::create()
                .name("idx_agent_memory_capsule_status")
                .table("agent_memory_capsule")
                .col("status")
                .to_owned(),
        )
        .await?;

    manager
        .create_index(
            Index::create()
                .name("idx_agent_memory_capsule_repair_status")
                .table("agent_memory_capsule")
                .col("repair_status")
                .to_owned(),
        )
        .await?;

    manager
        .create_index(
            Index::create()
                .name("idx_agent_memory_capsule_workspace_status")
                .table("agent_memory_capsule")
                .col("workspace_id")
                .col("status")
                .to_owned(),
        )
        .await?;

    Ok(())
}

async fn restore_episodic_capsule_schema(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
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
                .col(big_integer("active_frame_count").default(0))
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
        .await?;
    for (name, columns, unique) in [
        (
            "uidx_thread_episodic_capsules_ref",
            &["capsule_ref"][..],
            true,
        ),
        (
            "uidx_thread_episodic_capsules_segment",
            &["workspace_id", "thread_id", "segment_index"][..],
            true,
        ),
        (
            "idx_thread_episodic_capsules_write_state",
            &["workspace_id", "thread_id", "write_state"][..],
            false,
        ),
        (
            "idx_thread_episodic_capsules_workspace_status",
            &["workspace_id", "status"][..],
            false,
        ),
        (
            "idx_thread_episodic_capsules_repair_status",
            &["repair_status"][..],
            false,
        ),
    ] {
        let mut index = Index::create();
        index
            .name(name)
            .table(Alias::new("thread_episodic_capsules"));
        for column in columns {
            index.col(Alias::new(*column));
        }
        if unique {
            index.unique();
        }
        manager.create_index(index.to_owned()).await?;
    }
    // SeaQuery cannot express this historical SQLite partial-index predicate.
    manager.get_connection().execute_unprepared("CREATE UNIQUE INDEX uidx_thread_episodic_capsules_active_write ON thread_episodic_capsules(workspace_id,thread_id) WHERE write_state = 'active_write' AND status != 'deleted'").await?;
    Ok(())
}

// Trigger DDL is not represented by the installed SeaQuery API. These constant
// triggers maintain only the existing directory statistic. Changes roll back
// with the source item, including deletion/reset and already-persisted retries.
async fn install_directory_count(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    for sql in [
        "CREATE TRIGGER episodic_directory_count_insert AFTER INSERT ON thread_episodic_items WHEN new.status='active' AND new.search_text IS NOT NULL BEGIN UPDATE thread_episodic_thread_directory SET indexed_item_count=indexed_item_count+1 WHERE workspace_id=new.workspace_id AND thread_id=new.thread_id; END",
        "CREATE TRIGGER episodic_directory_count_delete AFTER DELETE ON thread_episodic_items WHEN old.status='active' AND old.search_text IS NOT NULL BEGIN UPDATE thread_episodic_thread_directory SET indexed_item_count=indexed_item_count-1 WHERE workspace_id=old.workspace_id AND thread_id=old.thread_id; END",
        "CREATE TRIGGER episodic_directory_count_update AFTER UPDATE OF status,search_text,workspace_id,thread_id ON thread_episodic_items WHEN ((old.status='active' AND old.search_text IS NOT NULL) IS NOT (new.status='active' AND new.search_text IS NOT NULL)) OR old.workspace_id IS NOT new.workspace_id OR old.thread_id IS NOT new.thread_id BEGIN UPDATE thread_episodic_thread_directory SET indexed_item_count=indexed_item_count-1 WHERE workspace_id=old.workspace_id AND thread_id=old.thread_id AND old.status='active' AND old.search_text IS NOT NULL; UPDATE thread_episodic_thread_directory SET indexed_item_count=indexed_item_count+1 WHERE workspace_id=new.workspace_id AND thread_id=new.thread_id AND new.status='active' AND new.search_text IS NOT NULL; END",
        // One initialization when a directory is genuinely created/recreated,
        // never on ordinary publication/conflict update. Existing occurrences
        // may predate the directory; serialized writer excludes a stale count.
        "CREATE TRIGGER episodic_directory_count_initialize AFTER INSERT ON thread_episodic_thread_directory BEGIN UPDATE thread_episodic_thread_directory SET indexed_item_count=(SELECT count(*) FROM thread_episodic_items i WHERE i.workspace_id=new.workspace_id AND i.thread_id=new.thread_id AND i.status='active' AND i.search_text IS NOT NULL) WHERE id=new.id; END",
    ] {
        manager.get_connection().execute_unprepared(sql).await?;
    }
    Ok(())
}
