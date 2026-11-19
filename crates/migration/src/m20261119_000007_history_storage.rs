use crate::drop_tables;
use sea_orm_migration::prelude::*;
use sea_orm_migration::schema::{big_integer, big_integer_null, json, json_null, text, text_null};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table("compaction_history_check")
                    .col(text("turn_id").primary_key())
                    .col(text("state").default("pending"))
                    .col(json_null("descriptor"))
                    .col(text_null("outcome"))
                    .col(big_integer("revision").default(0))
                    .col(big_integer("failures").default(0))
                    .col(big_integer("next_attempt_ms").default(0))
                    .col(big_integer_null("attempt_deadline_ms"))
                    .col(json_null("diagnostic"))
                    .col(text_null("config_hash"))
                    .col(big_integer("managed").default(0))
                    .col(text("purpose").default("execution"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("compaction_history_pending")
                    .table("compaction_history_check")
                    .col("state")
                    .col("turn_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("history_check_due")
                    .table("compaction_history_check")
                    .col("state")
                    .col("next_attempt_ms")
                    .col("turn_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("history_check_legacy")
                    .table("compaction_history_check")
                    .col("state")
                    .col("outcome")
                    .col("managed")
                    .col("next_attempt_ms")
                    .to_owned(),
            )
            .await?;

        manager.create_index(
            Index::create().name("compaction_root_history_due").table("compaction_history_check")
                .col("next_attempt_ms")
                .col("turn_id")
                .and_where(Expr::cust(r#""purpose" = 'root_conversation' AND "state" = 'pending' AND "managed" = 1"#))
                .to_owned(),
        ).await?;

        manager
            .create_table(
                Table::create()
                    .table("compaction_frozen_history")
                    .col(text("id").primary_key())
                    .col(text("workspace_id"))
                    .col(text("owner_thread"))
                    .col(text("identity_sha256"))
                    .col(big_integer("message_count"))
                    .col(big_integer("next_ordinal").default(0))
                    .col(big_integer("import_count").default(0))
                    .col(text("imports_sha256").default(
                        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                    ))
                    .col(big_integer("next_import").default(0))
                    .col(big_integer("ready").default(0))
                    .col(big_integer("storage_registered").default(0))
                    .col(big_integer("expired").default(0))
                    .check(Expr::cust(r#""expired" IN ( 0 , 1 )"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("frozen_storage_discovery")
                    .table("compaction_frozen_history")
                    .col("ready")
                    .col("storage_registered")
                    .col("id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("frozen_history_content")
                    .table("compaction_frozen_history")
                    .col("workspace_id")
                    .col("owner_thread")
                    .col("ready")
                    .col("identity_sha256")
                    .col("imports_sha256")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("frozen_expiry_discovery")
                    .table("compaction_frozen_history")
                    .col("expired")
                    .col("ready")
                    .col("id")
                    .to_owned(),
            )
            .await?;

        manager.create_table(
            Table::create().table("compaction_frozen_message_data")
                .col(text("manifest_id"))
                .col(big_integer("ordinal"))
                .col(text("reference_json"))
                .col(big_integer("bytes"))
                .primary_key(Index::create().col("manifest_id").col("ordinal"))
                .check(Expr::cust(r#"( "bytes" BETWEEN 0 AND 262144 ) AND octet_length("reference_json") = "bytes""#))
                .to_owned(),
        ).await?;

        manager.create_table(
            Table::create().table("compaction_frozen_import_data")
                .col(text("manifest_id"))
                .col(big_integer("ordinal"))
                .col(big_integer("message_ordinal"))
                .col(text("source_scope"))
                .col(text("source_id"))
                .col(text("source_version"))
                .col(text("source_thread"))
                .col(text("proof_json"))
                .col(big_integer("bytes"))
                .primary_key(Index::create().col("manifest_id").col("ordinal"))
                .index(Index::create().unique().col("manifest_id").col("message_ordinal").col("source_scope").col("source_id").col("source_version").col("source_thread"))
                .check(Expr::cust(r#"( "bytes" BETWEEN 0 AND 262144 ) AND octet_length("proof_json") = "bytes""#))
                .to_owned(),
        ).await?;

        manager
            .create_index(
                Index::create()
                    .name("compaction_frozen_import_source")
                    .table("compaction_frozen_import_data")
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
                    .table("compaction_task_output")
                    .col(text("task_run_turn_id").primary_key())
                    .col(text("task_id"))
                    .col(text("run_id"))
                    .col(text("workspace_id"))
                    .col(text("source_thread"))
                    .col(text("source_turn"))
                    .col(text("manifest_id"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("compaction_task_output_run")
                    .table("compaction_task_output")
                    .col("run_id")
                    .col("task_run_turn_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("task_output_frozen_root")
                    .table("compaction_task_output")
                    .col("manifest_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("compaction_delivery_output")
                    .col(text("delivery_id").primary_key())
                    .col(text("candidate_id"))
                    .col(text("task_run_turn_id"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("compaction_context")
                    .col(text("workspace_id"))
                    .col(text("thread_id"))
                    .col(text("owner").primary_key())
                    .col(text_null("head"))
                    .col(big_integer("format_version").default(1))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_compaction_lifecycle_context_thread")
                    .table("compaction_context")
                    .col("thread_id")
                    .col("owner")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("context_frozen_scope")
                    .table("compaction_context")
                    .col("workspace_id")
                    .col("owner")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("compaction_execution_stop")
                    .col(text("owner"))
                    .col(text("turn_id"))
                    .primary_key(Index::create().col("owner").col("turn_id"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("compaction_operation")
                    .col(text("id").primary_key())
                    .col(text("owner"))
                    .col(text("fingerprint"))
                    .col(text("status"))
                    .col(text("snapshot"))
                    .col(text_null("expected_head"))
                    .col(text_null("execution_turn"))
                    .col(big_integer("deadline_ms"))
                    .col(big_integer("attempts").default(0))
                    .col(big_integer("transient_retries").default(0))
                    .col(big_integer("correction").default(0))
                    .col(big_integer("next_portion").default(0))
                    .col(text_null("outcome"))
                    .index(Index::create().unique().col("owner").col("fingerprint"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("compaction_operation_owner_status")
                    .table("compaction_operation")
                    .col("owner")
                    .col("status")
                    .col("id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_compaction_lifecycle_execution")
                    .table("compaction_operation")
                    .col("execution_turn")
                    .col("id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_compaction_lifecycle_owner_execution")
                    .table("compaction_operation")
                    .col("owner")
                    .col("execution_turn")
                    .col("id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_compaction_lifecycle_owner")
                    .table("compaction_operation")
                    .col("owner")
                    .col("id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_compaction_lifecycle_running")
                    .table("compaction_operation")
                    .col("status")
                    .col("id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("compaction_operation_projection")
                    .col(text("operation_id").primary_key())
                    .col(text("manifest_id"))
                    .col(text("identity_sha256"))
                    .col(text("imports_sha256"))
                    .col(big_integer("import_count"))
                    .col(big_integer_null("terminal_generation"))
                    .check(Expr::cust(
                        r#""terminal_generation" IS NULL OR "terminal_generation" >= 0"#,
                    ))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("projection_frozen_origin")
                    .table("compaction_operation_projection")
                    .col("manifest_id")
                    .col("operation_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("compaction_checkpoint")
                    .col(text("id").primary_key())
                    .col(text("operation_id"))
                    .col(text("owner"))
                    .col(text_null("previous"))
                    .col(big_integer("portion"))
                    .col(text("summary"))
                    .col(text("identity_sha256"))
                    .col(json("selection"))
                    .col(big_integer("projection_version"))
                    .col(big_integer("format_version"))
                    .col(text("status"))
                    .col(big_integer("proof_version").default(0))
                    .index(Index::create().unique().col("operation_id").col("portion"))
                    .check(Expr::cust(r#""proof_version" IN ( 0 , 1 )"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("compaction_checkpoint_owner")
                    .table("compaction_checkpoint")
                    .col("owner")
                    .col("id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("checkpoint_proof_operation")
                    .table("compaction_checkpoint")
                    .col("operation_id")
                    .col("proof_version")
                    .col("status")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("checkpoint_incoming_previous")
                    .table("compaction_checkpoint")
                    .col("previous")
                    .col("id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("checkpoint_unproved_owner")
                    .table("compaction_checkpoint")
                    .col("owner")
                    .col("proof_version")
                    .col("operation_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("checkpoint_proof_discovery")
                    .table("compaction_checkpoint")
                    .col("proof_version")
                    .col("id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("compaction_coverage")
                    .col(text("checkpoint_id"))
                    .col(text("source_scope").extra(r#"COLLATE "C""#))
                    .col(text("source_id").extra(r#"COLLATE "C""#))
                    .col(text("source_version").extra(r#"COLLATE "C""#))
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
            .create_index(
                Index::create()
                    .name("coverage_incoming_checkpoint")
                    .table("compaction_coverage")
                    .col("source_id")
                    .col("source_scope")
                    .col("source_version")
                    .col("checkpoint_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("compaction_source_revision")
                    .col(text("source_id").primary_key())
                    .col(text("turn_id"))
                    .col(big_integer("revision"))
                    .col(big_integer("present"))
                    .col(big_integer("capture_order").default(0))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("compaction_source_revision_capture_order")
                    .table("compaction_source_revision")
                    .col("capture_order")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("compaction_item_revision")
                    .col(text("source_id").primary_key())
                    .col(text("turn_id"))
                    .col(big_integer("revision"))
                    .col(big_integer("present"))
                    .col(
                        ColumnDef::new("insertion_ordinal")
                            .big_integer()
                            .not_null()
                            .unique_key()
                            .extra("GENERATED ALWAYS AS IDENTITY"),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("compaction_attempt_observation")
                    .col(text("operation_id"))
                    .col(big_integer("attempt"))
                    .col(json("observation"))
                    .primary_key(Index::create().col("operation_id").col("attempt"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("compaction_runner_state")
                    .col(text("operation_id").primary_key())
                    .col(big_integer("generation"))
                    .col(json("state"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("compaction_manifest")
                    .col(text("operation_id"))
                    .col(big_integer("ordinal"))
                    .col(big_integer("unit_ordinal"))
                    .col(big_integer("reference_only"))
                    .col(text("source_thread"))
                    .col(text("source_scope"))
                    .col(text("source_id"))
                    .col(text("source_version"))
                    .primary_key(Index::create().col("operation_id").col("ordinal"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("compaction_manifest_unit")
                    .table("compaction_manifest")
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
                    .name("compaction_manifest_source")
                    .table("compaction_manifest")
                    .col("operation_id")
                    .col("reference_only")
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
                    .table("compaction_runner_plan")
                    .col(text("operation_id").primary_key())
                    .col(big_integer("source_count"))
                    .col(big_integer("reference_count"))
                    .col(json("descriptor"))
                    .col(big_integer("ready").default(0))
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("compaction_input_revision")
                    .col(text("source_id").primary_key())
                    .col(text("turn_id"))
                    .col(big_integer("revision"))
                    .col(big_integer("present"))
                    .col(big_integer("capture_order").default(0))
                    .col(
                        ColumnDef::new("insertion_ordinal")
                            .big_integer()
                            .not_null()
                            .unique_key()
                            .extra("GENERATED ALWAYS AS IDENTITY"),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("compaction_input_revision_capture_order")
                    .table("compaction_input_revision")
                    .col("capture_order")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("compaction_event_revision")
                    .col(text("source_id").primary_key())
                    .col(text("turn_id"))
                    .col(big_integer("revision"))
                    .col(big_integer("present"))
                    .col(big_integer("capture_order").default(0))
                    .col(big_integer_null("projection_revision"))
                    .col(text_null("item_id"))
                    .col(text_null("projection_kind"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("compaction_event_revision_capture_order")
                    .table("compaction_event_revision")
                    .col("capture_order")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("compaction_root_delivery_receipt")
                    .table("compaction_event_revision")
                    .col("turn_id")
                    .col("item_id")
                    .col("source_id")
                    .and_where(Expr::cust(
                        r#""present" = 1 AND "projection_revision" = "revision""#,
                    ))
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("compaction_projection_epoch")
                    .col(text("thread_id").primary_key())
                    .col(big_integer("version"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("compaction_turn_creation")
                    .col(
                        big_integer("sequence")
                            .primary_key()
                            .extra("GENERATED BY DEFAULT AS IDENTITY"),
                    )
                    .col(text("turn_id"))
                    .index(Index::create().unique().col("turn_id"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("compaction_task_basis_revision")
                    .col(text("run_id").primary_key())
                    .col(big_integer("revision"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("compaction_history_preparation")
                    .col(text("thread_id").primary_key())
                    .col(text_null("turn_id"))
                    .col(big_integer("upper_turn_rowid").default(0))
                    .col(big_integer("turn_rowid").default(0))
                    .col(big_integer("source_kind").default(0))
                    .col(big_integer("after_sequence").default(-1))
                    .col(big_integer("step").default(0))
                    .col(big_integer("inserted").default(0))
                    .col(big_integer("input_order").default(0))
                    .col(big_integer("event_order").default(0))
                    .col(big_integer("context_order").default(0))
                    .col(big_integer("ready").default(0))
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("compaction_frozen_layout")
                    .col(text("manifest_id"))
                    .col(big_integer("kind"))
                    .col(big_integer("active").default(0))
                    .col(big_integer("pending").default(1))
                    .col(text_null("candidate"))
                    .col(big_integer("compared").default(0))
                    .col(big_integer_null("copy_to"))
                    .col(big_integer("copy_next").default(0))
                    .col(big_integer("cleanup_to").default(0))
                    .col(big_integer("cleanup_next").default(0))
                    .col(big_integer("failed").default(0))
                    // Supplied by the publisher after checking the range pages.
                    .col(big_integer_null("verified_span_revision"))
                    .check(Expr::cust(r#""kind" IN (0, 1)"#))
                    .col(
                        ColumnDef::new("insertion_ordinal")
                            .big_integer()
                            .not_null()
                            .unique_key()
                            .extra("GENERATED ALWAYS AS IDENTITY"),
                    )
                    .primary_key(Index::create().col("manifest_id").col("kind"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("frozen_layout_pending")
                    .table("compaction_frozen_layout")
                    .col("pending")
                    .col("kind")
                    .col("manifest_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("frozen_layout_candidate")
                    .table("compaction_frozen_layout")
                    .col("candidate")
                    .col("kind")
                    .col("manifest_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("compaction_frozen_span")
                    .col(text("manifest_id"))
                    .col(big_integer("kind"))
                    .col(big_integer("start"))
                    .col(big_integer("end"))
                    .col(text("source_manifest"))
                    .col(
                        ColumnDef::new("insertion_ordinal")
                            .big_integer()
                            .not_null()
                            .unique_key()
                            .extra("GENERATED ALWAYS AS IDENTITY"),
                    )
                    .primary_key(Index::create().col("manifest_id").col("kind").col("start"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("frozen_span_source")
                    .table("compaction_frozen_span")
                    .col("source_manifest")
                    .col("kind")
                    .col("start")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("compaction_frozen_sweep_consumer_sources")
                    .table("compaction_frozen_span")
                    .col("manifest_id")
                    .col("kind")
                    .col("source_manifest")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("compaction_frozen_sweep_source_consumers")
                    .table("compaction_frozen_span")
                    .col("source_manifest")
                    .col("kind")
                    .col("manifest_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("compaction_lifecycle_sequence")
                    .col(big_integer("singleton").primary_key())
                    .col(big_integer("generation"))
                    .col(big_integer("seed_complete").default(0))
                    .check(Expr::cust(r#""singleton" = 1"#))
                    .check(Expr::cust(r#"generation >= 0"#))
                    .check(Expr::cust(r#""seed_complete" IN ( 0 , 1 )"#))
                    .to_owned(),
            )
            .await?;

        manager
            .exec_stmt(
                Query::insert()
                    .into_table("compaction_lifecycle_sequence")
                    .columns(["singleton", "generation", "seed_complete"])
                    .values_panic([1_i64.into(), 1_i64.into(), 0_i64.into()])
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("compaction_lifecycle_pending")
                    .col(text("operation_id").primary_key())
                    .col(big_integer("generation"))
                    .col(big_integer("eligible_at"))
                    .col(big_integer("retry_not_before").default(0))
                    .col(big_integer("due_at"))
                    .col(big_integer("attempts").default(0))
                    .col(text_null("claim_token"))
                    .check(Expr::cust(r#"generation > 0"#))
                    .check(Expr::cust(
                        r#"due_at = GREATEST(eligible_at, retry_not_before )"#,
                    ))
                    .check(Expr::cust(r#""attempts" BETWEEN 0 AND 16"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_compaction_lifecycle_due")
                    .table("compaction_lifecycle_pending")
                    .col("due_at")
                    .col("generation")
                    .col("operation_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("compaction_lifecycle_scope")
                    .col(text("kind"))
                    .col(text("owner"))
                    .col(text("turn_id"))
                    .col(big_integer("generation"))
                    .col(big_integer("due_at"))
                    .col(big_integer("attempts").default(0))
                    .col(text_null("claim_token"))
                    .col(text_null("cursor_id"))
                    .col(text_null("upper_id"))
                    .primary_key(Index::create().col("kind").col("owner").col("turn_id"))
                    .check(Expr::cust(
                        r#""kind" IN ( 'seed' , 'turn' , 'owner_turn' , 'owner' , 'thread' )"#,
                    ))
                    .check(Expr::cust(r#"generation > 0"#))
                    .check(Expr::cust(r#""attempts" BETWEEN 0 AND 16"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_compaction_lifecycle_scope_due")
                    .table("compaction_lifecycle_scope")
                    .col("due_at")
                    .col("generation")
                    .col("kind")
                    .to_owned(),
            )
            .await?;

        manager
            .exec_stmt(
                Query::insert()
                    .into_table("compaction_lifecycle_scope")
                    .columns([
                        "kind",
                        "owner",
                        "turn_id",
                        "generation",
                        "due_at",
                        "attempts",
                    ])
                    .values_panic([
                        "seed".into(),
                        "".into(),
                        "".into(),
                        1_i64.into(),
                        0_i64.into(),
                        0_i64.into(),
                    ])
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("compaction_checkpoint_replay_alias")
                    .col(text("checkpoint_id"))
                    .col(text("covered_thread"))
                    .col(text("covered_scope"))
                    .col(text("covered_id"))
                    .col(text("covered_version"))
                    .col(text("replay_thread"))
                    .col(text("replay_scope"))
                    .col(text("replay_id"))
                    .col(text("replay_version"))
                    .col(text_null("tool_item_id"))
                    .col(
                        ColumnDef::new("insertion_ordinal")
                            .big_integer()
                            .not_null()
                            .unique_key()
                            .extra("GENERATED ALWAYS AS IDENTITY"),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("checkpoint_replay_alias_identity")
                    .table("compaction_checkpoint_replay_alias")
                    .unique()
                    .col("checkpoint_id")
                    .col("covered_thread")
                    .col("covered_scope")
                    .col("covered_id")
                    .col("covered_version")
                    .col("replay_thread")
                    .col("replay_scope")
                    .col("replay_id")
                    .col("replay_version")
                    .col(Expr::cust(r#"( tool_item_id IS NULL )"#))
                    .col(Expr::cust(r#"COALESCE ( tool_item_id , '' )"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("compaction_checkpoint_event_input")
                    .col(text("checkpoint_id"))
                    .col(text("source_thread"))
                    .col(text("source_scope"))
                    .col(text("source_id"))
                    .col(text("source_version"))
                    .col(text("role"))
                    .col(
                        ColumnDef::new("insertion_ordinal")
                            .big_integer()
                            .not_null()
                            .unique_key()
                            .extra("GENERATED ALWAYS AS IDENTITY"),
                    )
                    .primary_key(
                        Index::create()
                            .col("checkpoint_id")
                            .col("source_thread")
                            .col("source_scope")
                            .col("source_id")
                            .col("source_version"),
                    )
                    .check(Expr::cust(
                        r#""role" IN ( 'authoritative' , 'deleted' , 'input_copy' )"#,
                    ))
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("compaction_checkpoint_import")
                    .col(text("checkpoint_id"))
                    .col(big_integer("import_ordinal"))
                    .col(big_integer("message_ordinal"))
                    .col(text("target_source_thread"))
                    .col(text("context_thread"))
                    .col(text_null("target_checkpoint_json"))
                    .col(text("proof_json"))
                    .col(big_integer("bytes"))
                    .primary_key(Index::create().col("checkpoint_id").col("import_ordinal"))
                    .check(Expr::cust(r#""import_ordinal" >= 0"#))
                    .check(Expr::cust(r#""message_ordinal" >= 0"#))
                    .check(Expr::cust(r#""bytes" >= 0"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("html_preview_frozen_index")
                    .col(text("manifest_id").primary_key())
                    .col(text("workspace_id"))
                    .col(text("identity_sha256"))
                    .col(big_integer("message_count"))
                    .col(big_integer("ready"))
                    .col(big_integer("expired"))
                    .col(big_integer("next_ordinal").default(0))
                    .col(big_integer("complete").default(0))
                    .col(big_integer("unknown").default(0))
                    .check(Expr::cust(r#""next_ordinal" >= 0"#))
                    .check(Expr::cust(r#""complete" IN ( 0 , 1 )"#))
                    .check(Expr::cust(r#""unknown" IN ( 0 , 1 )"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("html_preview_frozen_unready")
                    .table("html_preview_frozen_index")
                    .col("workspace_id")
                    .and_where(Expr::cust(
                        r#""expired" = 0 AND ("ready" <> 1 OR "complete" <> 1 OR "unknown" <> 0)"#,
                    ))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("html_preview_frozen_work")
                    .table("html_preview_frozen_index")
                    .col("manifest_id")
                    .and_where(Expr::cust(
                        r#"expired=0 AND ready=1 AND (complete<>1 OR unknown=1)"#,
                    ))
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("html_preview_index_backfill")
                    .col(big_integer("singleton").primary_key())
                    .col(big_integer("complete"))
                    .col(big_integer("inputs_complete").default(0))
                    .check(Expr::cust(r#""singleton" = 1"#))
                    .check(Expr::cust(r#""complete" IN ( 0 , 1 )"#))
                    .check(Expr::cust(r#""inputs_complete" IN ( 0 , 1 )"#))
                    .to_owned(),
            )
            .await?;

        manager
            .exec_stmt(
                Query::insert()
                    .into_table("html_preview_index_backfill")
                    .columns(["singleton", "complete", "inputs_complete"])
                    .values_panic([1_i64.into(), 0_i64.into(), 0_i64.into()])
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("html_preview_frozen_turn")
                    .col(text("manifest_id"))
                    .col(text("turn_id"))
                    .primary_key(Index::create().col("manifest_id").col("turn_id"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("html_preview_frozen_turn_lookup")
                    .table("html_preview_frozen_turn")
                    .col("turn_id")
                    .col("manifest_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("html_preview_frozen_result")
                    .col(text("manifest_id"))
                    .col(big_integer("ordinal"))
                    .col(text("reference_json"))
                    .col(text("turn_id"))
                    .col(text("source_id"))
                    .col(text("source_version"))
                    .col(text_null("archive_wire_sha256"))
                    .primary_key(Index::create().col("manifest_id").col("ordinal"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("html_preview_frozen_result_replay")
                    .table("html_preview_frozen_result")
                    .col("source_id")
                    .col("source_version")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("compaction_frozen_sweep_work")
                    .col(big_integer("work_row").primary_key())
                    .col(text("manifest_id"))
                    .col(big_integer("kind"))
                    .col(big_integer("revision").default(1))
                    .col(big_integer("pending").default(1))
                    .col(big_integer("retry_at").default(0))
                    .index(Index::create().unique().col("manifest_id").col("kind"))
                    .check(Expr::cust(r#""work_row" > 0"#))
                    .check(Expr::cust(r#""kind" IN ( 0 , 1 )"#))
                    .check(Expr::cust(r#""revision" > 0"#))
                    .check(Expr::cust(r#""pending" IN ( 0 , 1 )"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("compaction_frozen_sweep_work_pending")
                    .table("compaction_frozen_sweep_work")
                    .col("pending")
                    .col("kind")
                    .col("retry_at")
                    .col("manifest_id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("compaction_frozen_sweep_work_service")
                    .table("compaction_frozen_sweep_work")
                    .col("pending")
                    .col("kind")
                    .col("work_row")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("compaction_frozen_sweep_consumer")
                    .col(big_integer("consumer_row").primary_key())
                    .col(text("manifest_id"))
                    .col(big_integer("kind"))
                    .col(big_integer("revision").default(1))
                    .col(big_integer("pending").default(1))
                    .col(text_null("after_source"))
                    .index(Index::create().unique().col("manifest_id").col("kind"))
                    .check(Expr::cust(r#""consumer_row" > 0"#))
                    .check(Expr::cust(r#""kind" IN ( 0 , 1 )"#))
                    .check(Expr::cust(r#""revision" > 0"#))
                    .check(Expr::cust(r#""pending" IN ( 0 , 1 )"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("compaction_frozen_sweep_consumer_pending")
                    .table("compaction_frozen_sweep_consumer")
                    .col("pending")
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("compaction_frozen_sweep_fanout")
                    .col(big_integer("id").primary_key())
                    .col(big_integer("revision").default(1))
                    .col(big_integer("after_row").default(0))
                    .col(big_integer("until_row").default(0))
                    .check(Expr::cust(r#""id" = 1"#))
                    .check(Expr::cust(r#""revision" > 0"#))
                    .to_owned(),
            )
            .await?;

        manager
            .exec_stmt(
                Query::insert()
                    .into_table("compaction_frozen_sweep_fanout")
                    .columns(["id", "revision", "after_row", "until_row"])
                    .values_panic([1_i64.into(), 1_i64.into(), 0_i64.into(), 0_i64.into()])
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("compaction_frozen_sweep_bootstrap")
                    .col(big_integer("kind").primary_key())
                    .col(big_integer("started").default(0))
                    .col(text_null("upper_manifest"))
                    .col(text_null("after_manifest"))
                    .col(big_integer("complete").default(0))
                    .check(Expr::cust(r#""kind" IN ( 0 , 1 )"#))
                    .check(Expr::cust(r#""started" IN ( 0 , 1 )"#))
                    .check(Expr::cust(r#""complete" IN ( 0 , 1 )"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("compaction_frozen_sweep_bootstrap_pending")
                    .table("compaction_frozen_sweep_bootstrap")
                    .col("complete")
                    .col("kind")
                    .to_owned(),
            )
            .await?;

        manager
            .exec_stmt(
                Query::insert()
                    .into_table("compaction_frozen_sweep_bootstrap")
                    .columns(["kind", "started", "complete"])
                    .values_panic([0_i64.into(), 0_i64.into(), 0_i64.into()])
                    .to_owned(),
            )
            .await?;

        manager
            .exec_stmt(
                Query::insert()
                    .into_table("compaction_frozen_sweep_bootstrap")
                    .columns(["kind", "started", "complete"])
                    .values_panic([1_i64.into(), 0_i64.into(), 0_i64.into()])
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table("compaction_root_activity")
                    .col(text("thread_id").primary_key())
                    .col(text("user_turn_id"))
                    .col(big_integer("last_user_activity_ms"))
                    .check(Expr::cust(r#""last_user_activity_ms" >= 0"#))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("compaction_root_activity_turn")
                    .table("compaction_root_activity")
                    .unique()
                    .col("user_turn_id")
                    .to_owned(),
            )
            .await?;

        manager.create_table(
            Table::create().table("html_preview_io_work")
                .col(text("family"))
                .col(text("object_id"))
                .col(text("incarnation"))
                .col(text("requested_revision"))
                .col(text("pass_revision"))
                .col(text("state"))
                .col(text("workspace_id"))
                .col(big_integer("ordinal"))
                .col(big_integer_null("due_at"))
                .col(text_null("claim_token"))
                .col(text_null("cursor_id"))
                .col(big_integer_null("cursor_ordinal"))
                .col(text_null("upper_id"))
                .col(big_integer_null("upper_ordinal"))
                .col(big_integer("seed_version").default(0))
                .col(text_null("error_class"))
                .primary_key(Index::create().col("family").col("object_id").col("ordinal"))
                .check(Expr::cust(r#""family" IN ( 'witness' , 'copies' , 'seed_witness' , 'seed_copies' )"#))
                .check(Expr::cust(r#""state" IN ( 'ready' , 'timed' , 'running' , 'blocked' , 'done' , 'quarantine' )"#))
                .check(Expr::cust(r#"( "state" IN ( 'ready' , 'timed' , 'running' ) AND "due_at" IS NOT NULL ) OR ( "state" IN ( 'blocked' , 'done' , 'quarantine' ) AND "due_at" IS NULL )"#))
                .to_owned(),
        ).await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_html_preview_io_due")
                    .table("html_preview_io_work")
                    .col("due_at")
                    .col("family")
                    .col("object_id")
                    .col("ordinal")
                    .to_owned(),
            )
            .await?;

        manager
            .exec_stmt(
                Query::insert()
                    .into_table("html_preview_io_work")
                    .columns([
                        "family",
                        "object_id",
                        "incarnation",
                        "requested_revision",
                        "pass_revision",
                        "state",
                        "workspace_id",
                        "ordinal",
                        "due_at",
                        "seed_version",
                    ])
                    .values_panic([
                        "seed_witness".into(),
                        "".into(),
                        "seed_witness".into(),
                        "initial".into(),
                        "".into(),
                        "ready".into(),
                        "".into(),
                        0_i64.into(),
                        0_i64.into(),
                        0_i64.into(),
                    ])
                    .to_owned(),
            )
            .await?;

        manager
            .exec_stmt(
                Query::insert()
                    .into_table("html_preview_io_work")
                    .columns([
                        "family",
                        "object_id",
                        "incarnation",
                        "requested_revision",
                        "pass_revision",
                        "state",
                        "workspace_id",
                        "ordinal",
                        "due_at",
                        "seed_version",
                    ])
                    .values_panic([
                        "seed_copies".into(),
                        "".into(),
                        "seed_copies".into(),
                        "initial".into(),
                        "".into(),
                        "ready".into(),
                        "".into(),
                        0_i64.into(),
                        0_i64.into(),
                        0_i64.into(),
                    ])
                    .to_owned(),
            )
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        drop_tables(
            manager,
            &[
                "html_preview_io_work",
                "compaction_root_activity",
                "compaction_frozen_sweep_bootstrap",
                "compaction_frozen_sweep_fanout",
                "compaction_frozen_sweep_consumer",
                "compaction_frozen_sweep_work",
                "html_preview_frozen_result",
                "html_preview_frozen_turn",
                "html_preview_index_backfill",
                "html_preview_frozen_index",
                "compaction_checkpoint_import",
                "compaction_checkpoint_event_input",
                "compaction_checkpoint_replay_alias",
                "compaction_lifecycle_scope",
                "compaction_lifecycle_pending",
                "compaction_lifecycle_sequence",
                "compaction_frozen_span",
                "compaction_frozen_layout",
                "compaction_history_preparation",
                "compaction_task_basis_revision",
                "compaction_turn_creation",
                "compaction_projection_epoch",
                "compaction_event_revision",
                "compaction_input_revision",
                "compaction_runner_plan",
                "compaction_manifest",
                "compaction_runner_state",
                "compaction_attempt_observation",
                "compaction_item_revision",
                "compaction_source_revision",
                "compaction_coverage",
                "compaction_checkpoint",
                "compaction_operation_projection",
                "compaction_operation",
                "compaction_execution_stop",
                "compaction_context",
                "compaction_delivery_output",
                "compaction_task_output",
                "compaction_frozen_import_data",
                "compaction_frozen_message_data",
                "compaction_frozen_history",
                "compaction_history_check",
            ],
        )
        .await
    }
}
