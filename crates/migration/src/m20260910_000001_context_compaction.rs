use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Metadata only: canonical payloads remain in their existing tables.
        manager
            .create_table(
                Table::create()
                    .table(Alias::new("compaction_history_check"))
                    .if_not_exists()
                    .col(text("turn_id").primary_key())
                    .col(text("state").default("pending"))
                    .col(text("descriptor").null())
                    .col(text("outcome").null())
                    .foreign_key(
                        ForeignKey::create()
                            .from(
                                Alias::new("compaction_history_check"),
                                Alias::new("turn_id"),
                            )
                            .to(Alias::new("turn"), Alias::new("id"))
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("compaction_history_pending")
                    .table(Alias::new("compaction_history_check"))
                    .if_not_exists()
                    .col("state")
                    .col("turn_id")
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table(Alias::new("compaction_frozen_history"))
                    .if_not_exists()
                    .col(text("id").primary_key())
                    .col(text("workspace_id"))
                    .col(text("owner_thread"))
                    .col(text("identity_sha256"))
                    .col(integer("message_count"))
                    .col(integer("next_ordinal").default(0))
                    .col(integer("import_count").default(0))
                    .col(text("imports_sha256").default(
                        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                    ))
                    .col(integer("next_import").default(0))
                    .col(integer("ready").default(0))
                    .foreign_key(
                        ForeignKey::create()
                            .from(
                                Alias::new("compaction_frozen_history"),
                                Alias::new("workspace_id"),
                            )
                            .to(Alias::new("workspace"), Alias::new("id"))
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .from(
                                Alias::new("compaction_frozen_history"),
                                Alias::new("owner_thread"),
                            )
                            .to(Alias::new("thread"), Alias::new("id"))
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table(Alias::new("compaction_frozen_message"))
                    .if_not_exists()
                    .col(text("manifest_id"))
                    .col(integer("ordinal"))
                    .col(text("reference_json"))
                    .col(integer("bytes"))
                    .foreign_key(
                        ForeignKey::create()
                            .from(
                                Alias::new("compaction_frozen_message"),
                                Alias::new("manifest_id"),
                            )
                            .to(Alias::new("compaction_frozen_history"), Alias::new("id"))
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .check(
                        Expr::col("bytes").between(0, 262144).and(
                            Expr::expr(
                                Func::cust(Alias::new("length"))
                                    .arg(Expr::col("reference_json").cast_as(Alias::new("BLOB"))),
                            )
                            .eq(Expr::col("bytes")),
                        ),
                    )
                    .primary_key(Index::create().col("manifest_id").col("ordinal"))
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table(Alias::new("compaction_frozen_import"))
                    .if_not_exists()
                    .col(text("manifest_id"))
                    .col(integer("ordinal"))
                    .col(integer("message_ordinal"))
                    .col(text("source_scope"))
                    .col(text("source_id"))
                    .col(text("source_version"))
                    .col(text("source_thread"))
                    .col(text("proof_json"))
                    .col(integer("bytes"))
                    .foreign_key(
                        ForeignKey::create()
                            .from(
                                Alias::new("compaction_frozen_import"),
                                Alias::new("manifest_id"),
                            )
                            .to(Alias::new("compaction_frozen_history"), Alias::new("id"))
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .check(
                        Expr::col("bytes").between(0, 262144).and(
                            Expr::expr(
                                Func::cust(Alias::new("length"))
                                    .arg(Expr::col("proof_json").cast_as(Alias::new("BLOB"))),
                            )
                            .eq(Expr::col("bytes")),
                        ),
                    )
                    .primary_key(Index::create().col("manifest_id").col("ordinal"))
                    .index(
                        Index::create()
                            .unique()
                            .col("manifest_id")
                            .col("message_ordinal")
                            .col("source_scope")
                            .col("source_id")
                            .col("source_version")
                            .col("source_thread"),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("compaction_frozen_import_source")
                    .table(Alias::new("compaction_frozen_import"))
                    .if_not_exists()
                    .col("manifest_id")
                    .col("source_scope")
                    .col("source_id")
                    .col("source_version")
                    .col("source_thread")
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table(Alias::new("compaction_task_output"))
                    .if_not_exists()
                    .col(text("task_run_turn_id").primary_key())
                    .col(text("task_id"))
                    .col(text("run_id"))
                    .col(text("workspace_id"))
                    .col(text("source_thread"))
                    .col(text("source_turn"))
                    .col(text("manifest_id"))
                    .foreign_key(
                        ForeignKey::create()
                            .from(
                                Alias::new("compaction_task_output"),
                                Alias::new("task_run_turn_id"),
                            )
                            .to(Alias::new("task_run_turn"), Alias::new("id"))
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .from(Alias::new("compaction_task_output"), Alias::new("task_id"))
                            .to(Alias::new("task"), Alias::new("id"))
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .from(Alias::new("compaction_task_output"), Alias::new("run_id"))
                            .to(Alias::new("task_run"), Alias::new("id"))
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .from(
                                Alias::new("compaction_task_output"),
                                Alias::new("workspace_id"),
                            )
                            .to(Alias::new("workspace"), Alias::new("id"))
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .from(
                                Alias::new("compaction_task_output"),
                                Alias::new("source_thread"),
                            )
                            .to(Alias::new("thread"), Alias::new("id"))
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .from(
                                Alias::new("compaction_task_output"),
                                Alias::new("source_turn"),
                            )
                            .to(Alias::new("turn"), Alias::new("id"))
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .from(
                                Alias::new("compaction_task_output"),
                                Alias::new("manifest_id"),
                            )
                            .to(Alias::new("compaction_frozen_history"), Alias::new("id")),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("compaction_task_output_run")
                    .table(Alias::new("compaction_task_output"))
                    .if_not_exists()
                    .col("run_id")
                    .col("task_run_turn_id")
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table(Alias::new("compaction_delivery_output"))
                    .if_not_exists()
                    .col(text("delivery_id").primary_key())
                    .col(text("candidate_id"))
                    .col(text("task_run_turn_id"))
                    .foreign_key(
                        ForeignKey::create()
                            .from(
                                Alias::new("compaction_delivery_output"),
                                Alias::new("delivery_id"),
                            )
                            .to(Alias::new("task_delivery"), Alias::new("id"))
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .from(
                                Alias::new("compaction_delivery_output"),
                                Alias::new("candidate_id"),
                            )
                            .to(Alias::new("task_result_candidate"), Alias::new("id"))
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .from(
                                Alias::new("compaction_delivery_output"),
                                Alias::new("task_run_turn_id"),
                            )
                            .to(
                                Alias::new("compaction_task_output"),
                                Alias::new("task_run_turn_id"),
                            )
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table(Alias::new("compaction_context"))
                    .if_not_exists()
                    .col(text("workspace_id"))
                    .col(text("thread_id"))
                    .col(text("owner").primary_key())
                    .col(text("head").null())
                    .col(integer("format_version").default(1))
                    .foreign_key(
                        ForeignKey::create()
                            .from(Alias::new("compaction_context"), Alias::new("thread_id"))
                            .to(Alias::new("thread"), Alias::new("id"))
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table(Alias::new("compaction_execution_stop"))
                    .if_not_exists()
                    .col(text("owner"))
                    .col(text("turn_id"))
                    .foreign_key(
                        ForeignKey::create()
                            .from(Alias::new("compaction_execution_stop"), Alias::new("owner"))
                            .to(Alias::new("compaction_context"), Alias::new("owner"))
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .from(
                                Alias::new("compaction_execution_stop"),
                                Alias::new("turn_id"),
                            )
                            .to(Alias::new("turn"), Alias::new("id"))
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .primary_key(Index::create().col("owner").col("turn_id"))
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table(Alias::new("compaction_operation"))
                    .if_not_exists()
                    .col(text("id").primary_key())
                    .col(text("owner"))
                    .col(text("fingerprint"))
                    .col(text("status"))
                    .col(text("snapshot"))
                    .col(text("expected_head").null())
                    .col(text("execution_turn").null())
                    .col(integer("deadline_ms"))
                    .col(integer("attempts").default(0))
                    .col(integer("transient_retries").default(0))
                    .col(integer("correction").default(0))
                    .col(integer("next_portion").default(0))
                    .col(text("outcome").null())
                    .foreign_key(
                        ForeignKey::create()
                            .from(Alias::new("compaction_operation"), Alias::new("owner"))
                            .to(Alias::new("compaction_context"), Alias::new("owner"))
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .index(Index::create().unique().col("owner").col("fingerprint"))
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table(Alias::new("compaction_operation_projection"))
                    .if_not_exists()
                    .col(text("operation_id").primary_key())
                    .col(text("manifest_id"))
                    .col(text("identity_sha256"))
                    .col(text("imports_sha256"))
                    .col(integer("import_count"))
                    .foreign_key(
                        ForeignKey::create()
                            .from(
                                Alias::new("compaction_operation_projection"),
                                Alias::new("operation_id"),
                            )
                            .to(Alias::new("compaction_operation"), Alias::new("id"))
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .from(
                                Alias::new("compaction_operation_projection"),
                                Alias::new("manifest_id"),
                            )
                            .to(Alias::new("compaction_frozen_history"), Alias::new("id")),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table(Alias::new("compaction_checkpoint"))
                    .if_not_exists()
                    .col(text("id").primary_key())
                    .col(text("operation_id"))
                    .col(text("owner"))
                    .col(text("previous").null())
                    .col(integer("portion"))
                    .col(text("summary"))
                    .col(text("identity_sha256"))
                    .col(text("selection"))
                    .col(integer("projection_version"))
                    .col(integer("format_version"))
                    .col(text("status"))
                    .foreign_key(
                        ForeignKey::create()
                            .from(
                                Alias::new("compaction_checkpoint"),
                                Alias::new("operation_id"),
                            )
                            .to(Alias::new("compaction_operation"), Alias::new("id"))
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .index(Index::create().unique().col("operation_id").col("portion"))
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table(Alias::new("compaction_coverage"))
                    .if_not_exists()
                    .col(text("checkpoint_id"))
                    .col(text("source_scope"))
                    .col(text("source_id"))
                    .col(text("source_version"))
                    .foreign_key(
                        ForeignKey::create()
                            .from(
                                Alias::new("compaction_coverage"),
                                Alias::new("checkpoint_id"),
                            )
                            .to(Alias::new("compaction_checkpoint"), Alias::new("id"))
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .primary_key(
                        Index::create()
                            .col("checkpoint_id")
                            .col("source_scope")
                            .col("source_id"),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table(Alias::new("compaction_source_revision"))
                    .if_not_exists()
                    .col(text("source_id").primary_key())
                    .col(text("turn_id"))
                    .col(integer("revision"))
                    .col(integer("present"))
                    .col(integer("capture_order").default(0))
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table(Alias::new("compaction_item_revision"))
                    .if_not_exists()
                    .col(text("source_id").primary_key())
                    .col(text("turn_id"))
                    .col(integer("revision"))
                    .col(integer("present"))
                    .col(integer("capture_order").default(0))
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table(Alias::new("compaction_attempt_observation"))
                    .if_not_exists()
                    .col(text("operation_id"))
                    .col(integer("attempt"))
                    .col(text("observation"))
                    .foreign_key(
                        ForeignKey::create()
                            .from(
                                Alias::new("compaction_attempt_observation"),
                                Alias::new("operation_id"),
                            )
                            .to(Alias::new("compaction_operation"), Alias::new("id"))
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .primary_key(Index::create().col("operation_id").col("attempt"))
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table(Alias::new("compaction_runner_state"))
                    .if_not_exists()
                    .col(text("operation_id").primary_key())
                    .col(integer("generation"))
                    .col(text("state"))
                    .foreign_key(
                        ForeignKey::create()
                            .from(
                                Alias::new("compaction_runner_state"),
                                Alias::new("operation_id"),
                            )
                            .to(Alias::new("compaction_operation"), Alias::new("id"))
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table(Alias::new("compaction_manifest"))
                    .if_not_exists()
                    .col(text("operation_id"))
                    .col(integer("ordinal"))
                    .col(integer("unit_ordinal"))
                    .col(integer("reference_only"))
                    .col(text("source_thread"))
                    .col(text("source_scope"))
                    .col(text("source_id"))
                    .col(text("source_version"))
                    .foreign_key(
                        ForeignKey::create()
                            .from(
                                Alias::new("compaction_manifest"),
                                Alias::new("operation_id"),
                            )
                            .to(Alias::new("compaction_operation"), Alias::new("id"))
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .primary_key(Index::create().col("operation_id").col("ordinal"))
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table(Alias::new("compaction_runner_plan"))
                    .if_not_exists()
                    .col(text("operation_id").primary_key())
                    .col(integer("source_count"))
                    .col(integer("reference_count"))
                    .col(text("descriptor"))
                    .col(integer("ready").default(0))
                    .foreign_key(
                        ForeignKey::create()
                            .from(
                                Alias::new("compaction_runner_plan"),
                                Alias::new("operation_id"),
                            )
                            .to(Alias::new("compaction_operation"), Alias::new("id"))
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table(Alias::new("compaction_input_revision"))
                    .if_not_exists()
                    .col(text("source_id").primary_key())
                    .col(text("turn_id"))
                    .col(integer("revision"))
                    .col(integer("present"))
                    .col(integer("capture_order").default(0))
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table(Alias::new("compaction_event_revision"))
                    .if_not_exists()
                    .col(text("source_id").primary_key())
                    .col(text("turn_id"))
                    .col(integer("revision"))
                    .col(integer("present"))
                    .col(integer("capture_order").default(0))
                    .col(integer("projection_revision").null())
                    .col(text("item_id").null())
                    .col(text("projection_kind").null())
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table(Alias::new("compaction_projection_epoch"))
                    .if_not_exists()
                    .col(text("thread_id").primary_key())
                    .col(integer("version"))
                    .foreign_key(
                        ForeignKey::create()
                            .from(
                                Alias::new("compaction_projection_epoch"),
                                Alias::new("thread_id"),
                            )
                            .to(Alias::new("thread"), Alias::new("id"))
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table(Alias::new("compaction_turn_creation"))
                    .if_not_exists()
                    // Preserve SQLite INTEGER PRIMARY KEY nullability metadata;
                    // inserting NULL still allocates a fresh AUTOINCREMENT value.
                    .col(integer("sequence").null().primary_key().auto_increment())
                    .col(text("turn_id").unique_key())
                    .foreign_key(
                        ForeignKey::create()
                            .from(
                                Alias::new("compaction_turn_creation"),
                                Alias::new("turn_id"),
                            )
                            .to(Alias::new("turn"), Alias::new("id"))
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table(Alias::new("compaction_task_basis_revision"))
                    .if_not_exists()
                    .col(text("run_id").primary_key())
                    .col(integer("revision"))
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("compaction_source_revision_capture_order")
                    .table(Alias::new("compaction_source_revision"))
                    .if_not_exists()
                    .col("capture_order")
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("compaction_item_revision_capture_order")
                    .table(Alias::new("compaction_item_revision"))
                    .if_not_exists()
                    .col("capture_order")
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("compaction_turn_history_order")
                    .table(Alias::new("turn"))
                    .if_not_exists()
                    .col("thread_id")
                    .col("created_at")
                    .col("id")
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("compaction_input_revision_capture_order")
                    .table(Alias::new("compaction_input_revision"))
                    .if_not_exists()
                    .col("capture_order")
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("compaction_event_revision_capture_order")
                    .table(Alias::new("compaction_event_revision"))
                    .if_not_exists()
                    .col("capture_order")
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("compaction_task_creator")
                    .table(Alias::new("task"))
                    .if_not_exists()
                    .col("workspace_id")
                    .col("created_by_thread_id")
                    .col("created_by_turn_id")
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("compaction_delivery_turn")
                    .table(Alias::new("task_delivery"))
                    .if_not_exists()
                    .col("workspace_id")
                    .col("target_thread_id")
                    .col("delivered_turn_id")
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("compaction_delivery_source")
                    .table(Alias::new("task_delivery"))
                    .if_not_exists()
                    .col("task_id")
                    .col("workspace_id")
                    .col("target_thread_id")
                    .col("status")
                    .col("delivered_turn_id")
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("compaction_context_item_locator")
                    .table(Alias::new(
                        canonical_storage_table(manager, "turn_llm_context").await?,
                    ))
                    .if_not_exists()
                    .col("turn_id")
                    .col("source")
                    .col("item_id")
                    .col("sequence")
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("compaction_manifest_source")
                    .table(Alias::new("compaction_manifest"))
                    .if_not_exists()
                    .col("operation_id")
                    .col("reference_only")
                    .col("source_scope")
                    .col("source_id")
                    .col("source_version")
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("compaction_manifest_unit")
                    .table(Alias::new("compaction_manifest"))
                    .if_not_exists()
                    .col("operation_id")
                    .col("reference_only")
                    .col("unit_ordinal")
                    .col("ordinal")
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("compaction_operation_owner_status")
                    .table(Alias::new("compaction_operation"))
                    .if_not_exists()
                    .col("owner")
                    .col("status")
                    .col("id")
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("compaction_checkpoint_owner")
                    .table(Alias::new("compaction_checkpoint"))
                    .if_not_exists()
                    .col("owner")
                    .col("id")
                    .to_owned(),
            )
            .await?;
        // SeaQuery 1.0 has no CREATE VIEW builder (as in native_durable_delivery).
        // This SQLite view unifies live source versions without copying payloads.
        manager.get_connection().execute_unprepared(
            "CREATE VIEW IF NOT EXISTS compaction_live_sources AS SELECT 'context:'||r.turn_id AS source_scope,r.source_id,'revision:'||r.revision AS source_version,r.turn_id,t.thread_id,th.workspace_id FROM compaction_source_revision r JOIN turn_llm_context s ON s.id=r.source_id AND s.turn_id=r.turn_id JOIN turn t ON t.id=r.turn_id JOIN thread th ON th.id=t.thread_id WHERE r.present=1 UNION ALL SELECT 'item:'||r.turn_id AS source_scope,r.source_id,'item-revision:'||r.revision AS source_version,r.turn_id,t.thread_id,th.workspace_id FROM compaction_item_revision r JOIN turn_item s ON s.id=r.source_id AND s.turn_id=r.turn_id JOIN turn t ON t.id=r.turn_id JOIN thread th ON th.id=t.thread_id WHERE r.present=1 UNION ALL SELECT 'event:'||r.turn_id AS source_scope,r.source_id,'event-revision:'||r.revision AS source_version,r.turn_id,t.thread_id,th.workspace_id FROM compaction_event_revision r JOIN turn_event s ON s.id=r.source_id AND s.turn_id=r.turn_id JOIN turn t ON t.id=r.turn_id JOIN thread th ON th.id=t.thread_id WHERE r.present=1 UNION ALL SELECT 'input:'||r.turn_id,r.source_id,'input-revision:'||r.revision,r.turn_id,t.thread_id,th.workspace_id FROM compaction_input_revision r JOIN turn_input s ON s.id=r.source_id AND s.turn_id=r.turn_id JOIN turn t ON t.id=r.turn_id JOIN thread th ON th.id=t.thread_id WHERE r.present=1 UNION ALL SELECT 'checkpoint:'||p.owner,p.id,p.identity_sha256,NULL,c.thread_id,c.workspace_id FROM compaction_checkpoint p JOIN compaction_context c ON c.owner=p.owner LEFT JOIN compaction_projection_epoch e ON e.thread_id=c.thread_id WHERE (p.status='applied' OR (p.status='retained' AND EXISTS(SELECT 1 FROM compaction_operation committed WHERE committed.id=p.operation_id AND committed.status='completed'))) AND p.projection_version=COALESCE(e.version,0) UNION ALL SELECT 'task-basis:'||s.run_id,s.run_id,'task-basis-revision:'||COALESCE(r.revision,1),NULL,s.conversation_thread_id,s.workspace_id FROM task_run_conversation_snapshot s JOIN thread th ON th.id=s.conversation_thread_id AND th.workspace_id=s.workspace_id LEFT JOIN compaction_task_basis_revision r ON r.run_id=s.run_id WHERE substr(ltrim(s.history_json),1,1)='['"
        ).await?;

        // Canonical history can already be exposed through sqlite-zstd views.
        // As in incremental_read_model_repair, AFTER/BEFORE triggers belong on
        // physical storage; readers continue to use the canonical logical view.
        let mut storage_tables = Vec::new();
        for logical in ["turn_llm_context", "turn_item", "turn_input", "turn_event"] {
            storage_tables.push((logical, canonical_storage_table(manager, logical).await?));
        }

        // SQLite trigger bodies have no SeaQuery schema-builder equivalent.
        // Keep revision/epoch updates atomic with writes to canonical sources.
        for sql in [
            "CREATE TRIGGER IF NOT EXISTS compaction_history_cli_completed AFTER UPDATE OF status ON turn WHEN NEW.status='completed' AND OLD.status!='completed' AND EXISTS (SELECT 1 FROM turn_cli_runtime_binding b WHERE b.turn_id=NEW.id) BEGIN INSERT INTO compaction_history_check(turn_id) VALUES(NEW.id) ON CONFLICT(turn_id) DO NOTHING; END",
            "CREATE TRIGGER IF NOT EXISTS compaction_source_insert AFTER INSERT ON turn_llm_context BEGIN INSERT INTO compaction_source_revision(source_id,turn_id,revision,present,capture_order) VALUES (NEW.id,NEW.turn_id,1,1, (SELECT COALESCE(MAX(capture_order),0)+1 FROM compaction_source_revision)) ON CONFLICT(source_id) DO UPDATE SET revision=revision+1,present=1,turn_id=NEW.turn_id; END",
            "CREATE TRIGGER IF NOT EXISTS compaction_source_update AFTER UPDATE OF payload,turn_id,source,item_id,sequence ON turn_llm_context BEGIN INSERT INTO compaction_source_revision(source_id,turn_id,revision,present,capture_order) VALUES (NEW.id,NEW.turn_id,2,1, (SELECT COALESCE(MAX(capture_order),0)+1 FROM compaction_source_revision)) ON CONFLICT(source_id) DO UPDATE SET revision=revision+1,present=1,turn_id=NEW.turn_id; END",
            "CREATE TRIGGER IF NOT EXISTS compaction_source_delete AFTER DELETE ON turn_llm_context BEGIN INSERT INTO compaction_source_revision(source_id,turn_id,revision,present,capture_order) VALUES (OLD.id,OLD.turn_id,2,0, (SELECT COALESCE(MAX(capture_order),0)+1 FROM compaction_source_revision)) ON CONFLICT(source_id) DO UPDATE SET revision=revision+1,present=0; END",
            "CREATE TRIGGER IF NOT EXISTS compaction_item_insert AFTER INSERT ON turn_item BEGIN INSERT INTO compaction_item_revision(source_id,turn_id,revision,present,capture_order) VALUES (NEW.id,NEW.turn_id,1,1, (SELECT COALESCE(MAX(capture_order),0)+1 FROM compaction_item_revision)) ON CONFLICT(source_id) DO UPDATE SET revision=revision+1,present=1,turn_id=NEW.turn_id; END",
            "CREATE TRIGGER IF NOT EXISTS compaction_item_update AFTER UPDATE OF payload,turn_id,item_id,item_type,status ON turn_item BEGIN INSERT INTO compaction_item_revision(source_id,turn_id,revision,present,capture_order) VALUES (NEW.id,NEW.turn_id,2,1, (SELECT COALESCE(MAX(capture_order),0)+1 FROM compaction_item_revision)) ON CONFLICT(source_id) DO UPDATE SET revision=revision+1,present=1,turn_id=NEW.turn_id; END",
            "CREATE TRIGGER IF NOT EXISTS compaction_item_delete AFTER DELETE ON turn_item BEGIN INSERT INTO compaction_item_revision(source_id,turn_id,revision,present,capture_order) VALUES (OLD.id,OLD.turn_id,2,0, (SELECT COALESCE(MAX(capture_order),0)+1 FROM compaction_item_revision)) ON CONFLICT(source_id) DO UPDATE SET revision=revision+1,present=0; END",
            "CREATE TRIGGER IF NOT EXISTS compaction_input_insert AFTER INSERT ON turn_input BEGIN INSERT INTO compaction_input_revision(source_id,turn_id,revision,present,capture_order) VALUES (NEW.id,NEW.turn_id,1,1, (SELECT COALESCE(MAX(capture_order),0)+1 FROM compaction_input_revision)) ON CONFLICT(source_id) DO UPDATE SET revision=revision+1,turn_id=NEW.turn_id,present=1; END",
            "CREATE TRIGGER IF NOT EXISTS compaction_input_update AFTER UPDATE OF payload,text,turn_id,input_type,input_index ON turn_input BEGIN INSERT INTO compaction_input_revision(source_id,turn_id,revision,present,capture_order) VALUES (NEW.id,NEW.turn_id,2,1, (SELECT COALESCE(MAX(capture_order),0)+1 FROM compaction_input_revision)) ON CONFLICT(source_id) DO UPDATE SET revision=revision+1,turn_id=NEW.turn_id,present=1; END",
            "CREATE TRIGGER IF NOT EXISTS compaction_input_delete AFTER DELETE ON turn_input BEGIN INSERT INTO compaction_input_revision(source_id,turn_id,revision,present,capture_order) VALUES (OLD.id,OLD.turn_id,2,0, (SELECT COALESCE(MAX(capture_order),0)+1 FROM compaction_input_revision)) ON CONFLICT(source_id) DO UPDATE SET revision=revision+1,present=0; END",
            "CREATE TRIGGER IF NOT EXISTS compaction_input_epoch_update AFTER UPDATE OF payload,text,turn_id,input_type,input_index ON turn_input BEGIN INSERT INTO compaction_projection_epoch(thread_id,version) SELECT DISTINCT t.thread_id,1 FROM turn t JOIN thread th ON th.id=t.thread_id WHERE t.id IN (OLD.turn_id,NEW.turn_id) ON CONFLICT(thread_id) DO UPDATE SET version=version+1; END",
            "CREATE TRIGGER IF NOT EXISTS compaction_input_epoch_delete BEFORE DELETE ON turn_input BEGIN INSERT INTO compaction_projection_epoch(thread_id,version) SELECT t.thread_id,1 FROM turn t JOIN thread th ON th.id=t.thread_id WHERE t.id=OLD.turn_id ON CONFLICT(thread_id) DO UPDATE SET version=version+1; END",
            "CREATE TRIGGER IF NOT EXISTS compaction_event_insert AFTER INSERT ON turn_event BEGIN INSERT INTO compaction_event_revision(source_id,turn_id,revision,present,capture_order) VALUES (NEW.id,NEW.turn_id,1,1, (SELECT COALESCE(MAX(capture_order),0)+1 FROM compaction_event_revision)) ON CONFLICT(source_id) DO UPDATE SET revision=revision+1,present=1,turn_id=NEW.turn_id; END",
            "CREATE TRIGGER IF NOT EXISTS compaction_event_update AFTER UPDATE OF payload,turn_id,event_type,sequence ON turn_event BEGIN INSERT INTO compaction_event_revision(source_id,turn_id,revision,present,capture_order) VALUES (NEW.id,NEW.turn_id,2,1, (SELECT COALESCE(MAX(capture_order),0)+1 FROM compaction_event_revision)) ON CONFLICT(source_id) DO UPDATE SET revision=revision+1,present=1,turn_id=NEW.turn_id; END",
            "CREATE TRIGGER IF NOT EXISTS compaction_event_delete AFTER DELETE ON turn_event BEGIN INSERT INTO compaction_event_revision(source_id,turn_id,revision,present,capture_order) VALUES (OLD.id,OLD.turn_id,2,0, (SELECT COALESCE(MAX(capture_order),0)+1 FROM compaction_event_revision)) ON CONFLICT(source_id) DO UPDATE SET revision=revision+1,present=0; END",
            "CREATE TRIGGER IF NOT EXISTS compaction_turn_creation_insert AFTER INSERT ON turn BEGIN INSERT INTO compaction_turn_creation(turn_id) VALUES (NEW.id); END",
            "CREATE TRIGGER IF NOT EXISTS compaction_context_epoch_update AFTER UPDATE OF payload,turn_id,source,item_id,sequence ON turn_llm_context WHEN 1 BEGIN INSERT INTO compaction_projection_epoch(thread_id,version) SELECT DISTINCT t.thread_id,1 FROM turn t JOIN thread th ON th.id=t.thread_id WHERE t.id IN (OLD.turn_id,NEW.turn_id) ON CONFLICT(thread_id) DO UPDATE SET version=version+1; END",
            "CREATE TRIGGER IF NOT EXISTS compaction_context_epoch_delete BEFORE DELETE ON turn_llm_context WHEN 1 BEGIN INSERT INTO compaction_projection_epoch(thread_id,version) SELECT t.thread_id,1 FROM turn t JOIN thread th ON th.id=t.thread_id WHERE t.id=OLD.turn_id ON CONFLICT(thread_id) DO UPDATE SET version=version+1; END",
            "CREATE TRIGGER IF NOT EXISTS compaction_event_epoch_update AFTER UPDATE OF payload,turn_id,event_type,sequence ON turn_event WHEN 1 BEGIN INSERT INTO compaction_projection_epoch(thread_id,version) SELECT DISTINCT t.thread_id,1 FROM turn t JOIN thread th ON th.id=t.thread_id WHERE t.id IN (OLD.turn_id,NEW.turn_id) ON CONFLICT(thread_id) DO UPDATE SET version=version+1; END",
            "CREATE TRIGGER IF NOT EXISTS compaction_event_epoch_delete BEFORE DELETE ON turn_event WHEN 1 BEGIN INSERT INTO compaction_projection_epoch(thread_id,version) SELECT t.thread_id,1 FROM turn t JOIN thread th ON th.id=t.thread_id WHERE t.id=OLD.turn_id ON CONFLICT(thread_id) DO UPDATE SET version=version+1; END",
            "CREATE TRIGGER IF NOT EXISTS compaction_item_epoch_update AFTER UPDATE OF payload,turn_id,item_id,item_type,status ON turn_item WHEN OLD.item_type='user_message' OR (OLD.item_type IN ('command_execution','file_change','web_search','web_fetch','download','dynamic_tool_call') AND (OLD.status IS NULL OR OLD.status IN ('completed','failed'))) BEGIN INSERT INTO compaction_projection_epoch(thread_id,version) SELECT DISTINCT t.thread_id,1 FROM turn t JOIN thread th ON th.id=t.thread_id WHERE t.id IN (OLD.turn_id,NEW.turn_id) ON CONFLICT(thread_id) DO UPDATE SET version=version+1; END",
            "CREATE TRIGGER IF NOT EXISTS compaction_item_epoch_delete BEFORE DELETE ON turn_item WHEN OLD.item_type <> 'system_event' BEGIN INSERT INTO compaction_projection_epoch(thread_id,version) SELECT t.thread_id,1 FROM turn t JOIN thread th ON th.id=t.thread_id WHERE t.id=OLD.turn_id ON CONFLICT(thread_id) DO UPDATE SET version=version+1; END",
            "CREATE TRIGGER IF NOT EXISTS compaction_turn_epoch_delete BEFORE DELETE ON turn BEGIN INSERT INTO compaction_projection_epoch(thread_id,version) SELECT id,1 FROM thread WHERE id=OLD.thread_id ON CONFLICT(thread_id) DO UPDATE SET version=version+1; END",
            "CREATE TRIGGER IF NOT EXISTS compaction_task_basis_insert AFTER INSERT ON task_run_conversation_snapshot BEGIN INSERT INTO compaction_task_basis_revision(run_id,revision) VALUES (NEW.run_id,1) ON CONFLICT(run_id) DO UPDATE SET revision=revision+1; END",
            "CREATE TRIGGER IF NOT EXISTS compaction_task_basis_update AFTER UPDATE ON task_run_conversation_snapshot BEGIN INSERT INTO compaction_task_basis_revision(run_id,revision) VALUES (OLD.run_id,2) ON CONFLICT(run_id) DO UPDATE SET revision=revision+1; INSERT INTO compaction_projection_epoch(thread_id,version) SELECT id,1 FROM thread WHERE id IN (OLD.conversation_thread_id,NEW.conversation_thread_id) ON CONFLICT(thread_id) DO UPDATE SET version=version+1; END",
            "CREATE TRIGGER IF NOT EXISTS compaction_task_basis_delete BEFORE DELETE ON task_run_conversation_snapshot BEGIN INSERT INTO compaction_task_basis_revision(run_id,revision) VALUES (OLD.run_id,2) ON CONFLICT(run_id) DO UPDATE SET revision=revision+1; INSERT INTO compaction_projection_epoch(thread_id,version) SELECT id,1 FROM thread WHERE id=OLD.conversation_thread_id ON CONFLICT(thread_id) DO UPDATE SET version=version+1; END",
        ] {
            let mut trigger = sql.to_owned();
            for (logical, storage) in &storage_tables {
                trigger =
                    trigger.replace(&format!(" ON {logical} "), &format!(" ON \"{storage}\" "));
            }
            manager
                .get_connection()
                .execute_unprepared(&trigger)
                .await?;
        }
        Ok(())
    }
    async fn down(&self, _: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Custom(
            "compaction summaries and coverage cannot be discarded by downgrade".into(),
        ))
    }
}

/// Only fixed canonical names supplied above reach this helper. Preserve the
/// backing-table trigger pattern used by the existing read-model migration.
async fn canonical_storage_table(
    manager: &SchemaManager<'_>,
    logical: &str,
) -> Result<String, DbErr> {
    let backing = format!("_{logical}_zstd");
    for name in [backing.as_str(), logical] {
        let row = manager
            .get_connection()
            .query_one(
                &Query::select()
                    .column("name")
                    .from("sqlite_master")
                    .and_where(Expr::col("name").eq(name))
                    .and_where(Expr::col("type").eq("table"))
                    .limit(1)
                    .to_owned(),
            )
            .await?;
        if row.is_some() {
            return Ok(name.to_owned());
        }
    }
    Err(DbErr::Migration(format!(
        "compaction migration cannot find canonical storage for {logical}"
    )))
}
